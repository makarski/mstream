use crate::config::{Encoding, service_config::MongoDbWriteMode};
use mongodb::{
    Database,
    bson::{self, Bson, RawDocumentBuf, doc},
    error::Error,
};
use tokio::task::block_in_place;

pub struct MongoDbPersister {
    db: Database,
    write_mode: MongoDbWriteMode,
}

impl MongoDbPersister {
    pub fn new(db: Database, write_mode: MongoDbWriteMode) -> Self {
        Self { db, write_mode }
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
            Encoding::Bson => self.persist_one_bson(b, coll_name).await,
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

        if !matches!(content_type, BatchContentType::Bson) {
            return Err(Error::custom(format!(
                "only BSON framed batch persistence is supported. got: {:?}",
                content_type,
            )));
        }

        let res = match self.write_mode {
            MongoDbWriteMode::Insert => self.persist_framed_insert(items, coll_name).await,
            MongoDbWriteMode::Replace => self.persist_framed_upsert(items, coll_name).await,
        };

        Ok(res?)
    }

    async fn persist_framed_insert(
        &self,
        items: Vec<Vec<u8>>,
        coll_name: &str,
    ) -> Result<String, Error> {
        let coll = self.db.collection::<RawDocumentBuf>(coll_name);
        let docs: Result<Vec<RawDocumentBuf>, _> = block_in_place(|| {
            items
                .into_iter()
                .map(|item| RawDocumentBuf::from_bytes(item))
                .collect()
        });

        let docs = docs.map_err(|e| Error::custom(format!("invalid BSON in batch: {}", e)))?;
        let result = coll.insert_many(docs).await?;
        Ok(format!("{}", result.inserted_ids.len()))
    }

    async fn persist_framed_upsert(
        &self,
        items: Vec<Vec<u8>>,
        coll_name: &str,
    ) -> Result<String, Error> {
        let coll = self.db.collection::<RawDocumentBuf>(coll_name);

        let docs: Result<Vec<(bson::Document, RawDocumentBuf)>, Error> = block_in_place(|| {
            items
                .into_iter()
                .map(|item| {
                    let raw_doc = RawDocumentBuf::from_bytes(item)
                        .map_err(|e| Error::custom(format!("invalid BSON in batch: {}", e)))?;

                    let id_bson = self.get_id_field(&raw_doc)?;
                    let filter_doc = doc! { "_id": id_bson };
                    Ok((filter_doc, raw_doc))
                })
                .collect()
        });

        let docs = docs?;

        let upserts: Vec<_> = docs
            .into_iter()
            .map(|(filter, doc)| coll.replace_one(filter, doc).upsert(true).into_future())
            .collect();

        let results = futures::future::try_join_all(upserts).await?;
        let upserted_count = results
            .into_iter()
            .filter(|res| res.upserted_id.is_some())
            .count();

        Ok(format!("{}", upserted_count))
    }

    async fn persist_one_bson(&self, b: Vec<u8>, coll_name: &str) -> Result<String, Error> {
        let coll = self.db.collection::<RawDocumentBuf>(coll_name);
        let doc = block_in_place(|| RawDocumentBuf::from_bytes(b))
            .map_err(|e| Error::custom(format!("failed to parse BSON: {}", e)))?;

        if matches!(self.write_mode, MongoDbWriteMode::Insert) {
            let id = coll.insert_one(doc).await?.inserted_id.to_string();
            return Ok(id);
        }

        let id_bson = self.get_id_field(&doc)?;

        let filter = doc! { "_id": id_bson };
        let res_id = coll
            .replace_one(filter, doc)
            .upsert(true)
            .await?
            .upserted_id
            .map(|id| id.to_string())
            .unwrap_or_else(|| "replaced".to_string());

        Ok(res_id)
    }

    fn get_id_field(&self, doc: &RawDocumentBuf) -> Result<Bson, Error> {
        get_id_field(doc)
    }
}

fn get_id_field(doc: &RawDocumentBuf) -> Result<Bson, Error> {
    let doc_id = doc
        .get("_id")
        .map_err(|err| Error::custom(format!("failed to read _id: {}", err)))?
        .ok_or_else(|| Error::custom(format!("_id field must be defined for the upsert mode")))?;

    let id_bson: Bson = doc_id
        .to_raw_bson()
        .try_into()
        .map_err(|e| Error::custom(format!("failed to convert _id to Bson: {}", e)))?;

    Ok(id_bson)
}

#[cfg(test)]
mod tests {
    use super::*;
    use mongodb::bson::oid::ObjectId;

    mod get_id_field_tests {
        use super::*;

        #[test]
        fn extracts_objectid() {
            let oid = ObjectId::new();
            let doc = doc! { "_id": oid, "name": "test" };
            let raw = RawDocumentBuf::from_document(&doc).unwrap();

            let result = get_id_field(&raw).unwrap();

            assert_eq!(result, Bson::ObjectId(oid));
        }

        #[test]
        fn extracts_string_id() {
            let doc = doc! { "_id": "custom-id-123", "data": "value" };
            let raw = RawDocumentBuf::from_document(&doc).unwrap();

            let result = get_id_field(&raw).unwrap();

            assert_eq!(result, Bson::String("custom-id-123".to_string()));
        }

        #[test]
        fn extracts_int_id() {
            let doc = doc! { "_id": 42_i64, "count": 1 };
            let raw = RawDocumentBuf::from_document(&doc).unwrap();

            let result = get_id_field(&raw).unwrap();

            assert_eq!(result, Bson::Int64(42));
        }

        #[test]
        fn errors_on_missing_id() {
            let doc = doc! { "name": "no id here" };
            let raw = RawDocumentBuf::from_document(&doc).unwrap();

            let result = get_id_field(&raw);

            assert!(result.is_err(), "expected error for doc without _id");
        }
    }
}
