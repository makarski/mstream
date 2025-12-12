use crate::config::Encoding;
use mongodb::{
    bson::{self, Document},
    error::Error,
    Collection, Database,
};
use tokio::task::block_in_place;

pub struct MongoDbPersister {
    db: Database,
}

impl MongoDbPersister {
    pub fn new(db: Database) -> Self {
        Self { db }
    }

    pub async fn persist2(
        &self,
        b: Vec<u8>,
        coll_name: &str,
        encoding: &Encoding,
        is_framed_batch: bool,
    ) -> Result<String, Error> {
        let coll = self.db.collection::<Document>(coll_name);

        if is_framed_batch {
            return self.persist_framed(b, coll).await;
        }

        match encoding {
            Encoding::Json => self.persist_json(b, coll).await,
            Encoding::Bson => self.persist_bson(b, coll).await,
            Encoding::Avro => Err(Error::custom(
                "Avro persistence not implemented (requires schema)",
            )),
        }
    }

    async fn persist_framed(
        &self,
        b: Vec<u8>,
        coll: Collection<Document>,
    ) -> Result<String, Error> {
        use crate::encoding::framed::BatchContentType;

        let (items, content_type) = block_in_place(|| crate::encoding::framed::decode(&b))
            .map_err(|e| Error::custom(format!("failed to decode FramedBytes batch: {}", e)))?;

        if items.is_empty() {
            return Ok("empty batch".to_string());
        }

        let docs: Result<Vec<Document>, _> = block_in_place(|| {
            items
                .into_iter()
                .map(|item| match content_type {
                    BatchContentType::Bson => bson::from_slice(&item)
                        .map_err(|e| Error::custom(format!("failed to parse BSON item: {}", e))),
                    BatchContentType::Json => {
                        let json_val: serde_json::Value =
                            serde_json::from_slice(&item).map_err(|e| {
                                Error::custom(format!("failed to parse JSON item: {}", e))
                            })?;
                        bson::to_document(&json_val).map_err(|e| {
                            Error::custom(format!("failed to convert JSON item to BSON: {}", e))
                        })
                    }
                    BatchContentType::Avro => {
                        Err(Error::custom("Avro batch persistence not yet implemented"))
                    }
                    BatchContentType::Raw => {
                        Err(Error::custom("Raw batch persistence not supported"))
                    }
                })
                .collect()
        });

        let docs = docs?;
        let result = coll.insert_many(docs).await?;
        Ok(format!("inserted {} documents", result.inserted_ids.len()))
    }

    async fn persist_json(&self, b: Vec<u8>, coll: Collection<Document>) -> Result<String, Error> {
        let json_val = block_in_place(|| serde_json::from_slice::<serde_json::Value>(&b))
            .map_err(|e| Error::custom(format!("failed to parse JSON: {}", e)))?;

        match json_val {
            serde_json::Value::Array(items) => {
                if items.is_empty() {
                    return Ok("empty batch".to_string());
                }
                let docs: Result<Vec<Document>, _> = block_in_place(|| {
                    items
                        .into_iter()
                        .map(|item| bson::to_document(&item))
                        .collect()
                });

                let docs = docs.map_err(|e| {
                    Error::custom(format!("failed to convert JSON array to BSON docs: {}", e))
                })?;

                let result = coll.insert_many(docs).await?;
                Ok(format!("inserted {} documents", result.inserted_ids.len()))
            }
            serde_json::Value::Object(_) => {
                let doc = block_in_place(|| bson::to_document(&json_val)).map_err(|e| {
                    Error::custom(format!("failed to convert JSON object to BSON: {}", e))
                })?;

                let result = coll.insert_one(doc).await?;
                Ok(result.inserted_id.to_string())
            }
            _ => Err(Error::custom(
                "received JSON primitive, cannot persist to MongoDB",
            )),
        }
    }

    async fn persist_bson(&self, b: Vec<u8>, coll: Collection<Document>) -> Result<String, Error> {
        let doc = block_in_place(|| bson::from_slice::<Document>(&b))
            .map_err(|e| Error::custom(format!("failed to parse BSON: {}", e)))?;
        let result = coll.insert_one(doc).await?;

        Ok(result.inserted_id.to_string())
    }
}
