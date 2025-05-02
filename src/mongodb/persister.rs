use mongodb::{
    bson::{self, Document},
    error::Error,
    Database,
};

pub struct MongoDbPersister {
    db: Database,
}

impl MongoDbPersister {
    pub fn new(db: Database) -> Self {
        Self { db }
    }

    // todo: clean up - decide whether to always rely on the raw_bytes or
    // intriduce custom doc support
    // this is due to the fact that schema applies conversions to raw bytes
    pub async fn persist(&self, doc: Document, coll_name: &str) -> Result<String, Error> {
        let coll = self.db.collection(coll_name);
        let result = coll.insert_one(doc, None).await?;

        Ok(result.inserted_id.to_string())
    }

    pub async fn persist2(&self, b: Vec<u8>, coll_name: &str) -> Result<String, Error> {
        let doc = bson::from_slice::<Document>(&b)?;
        let coll = self.db.collection(coll_name);
        let result = coll.insert_one(doc, None).await?;

        Ok(result.inserted_id.to_string())
    }
}
