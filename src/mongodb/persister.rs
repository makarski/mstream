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

    pub async fn persist2(&self, b: Vec<u8>, coll_name: &str) -> Result<String, Error> {
        let doc = bson::from_slice::<Document>(&b)?;
        let coll = self.db.collection(coll_name);
        let result = coll.insert_one(doc).await?;

        Ok(result.inserted_id.to_string())
    }
}
