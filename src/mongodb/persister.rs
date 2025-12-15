use crate::config::Encoding;
use mongodb::{bson::RawDocumentBuf, error::Error, Database};
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
        if is_framed_batch {
            return self.persist_framed(b, coll_name).await;
        }

        match encoding {
            Encoding::Json => Err(Error::custom(
                "JSON persistence is disabled. Convert to BSON in middleware.",
            )),
            Encoding::Bson => self.persist_bson(b, coll_name).await,
            Encoding::Avro => Err(Error::custom(
                "Avro persistence not implemented (requires schema)",
            )),
        }
    }

    async fn persist_framed(&self, b: Vec<u8>, coll_name: &str) -> Result<String, Error> {
        use crate::encoding::framed::BatchContentType;

        let (items, content_type) = block_in_place(|| crate::encoding::framed::decode(&b))
            .map_err(|e| Error::custom(format!("failed to decode FramedBytes batch: {}", e)))?;

        if items.is_empty() {
            return Ok("empty batch".to_string());
        }

        match content_type {
            BatchContentType::Bson => {
                let coll = self.db.collection::<RawDocumentBuf>(coll_name);
                let docs: Result<Vec<RawDocumentBuf>, _> = block_in_place(|| {
                    items
                        .into_iter()
                        .map(|item| RawDocumentBuf::from_bytes(item))
                        .collect()
                });
                let docs =
                    docs.map_err(|e| Error::custom(format!("invalid BSON in batch: {}", e)))?;
                let result = coll.insert_many(docs).await?;
                Ok(format!("inserted {} documents", result.inserted_ids.len()))
            }
            BatchContentType::Json => Err(Error::custom(
                "JSON batch persistence is disabled. Convert to BSON in middleware.",
            )),
            BatchContentType::Avro => {
                Err(Error::custom("Avro batch persistence not yet implemented"))
            }
            BatchContentType::Raw => Err(Error::custom("Raw batch persistence not supported")),
        }
    }

    async fn persist_bson(&self, b: Vec<u8>, coll_name: &str) -> Result<String, Error> {
        let coll = self.db.collection::<RawDocumentBuf>(coll_name);
        let doc = block_in_place(|| RawDocumentBuf::from_bytes(b))
            .map_err(|e| Error::custom(format!("failed to parse BSON: {}", e)))?;
        let result = coll.insert_one(doc).await?;

        Ok(result.inserted_id.to_string())
    }
}
