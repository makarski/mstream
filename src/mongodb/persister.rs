use mongodb::{bson::Document, error::Error, Database};

pub struct MongoDbPersister {
    db: Database,
}

impl MongoDbPersister {
    pub fn new(db: Database) -> Self {
        Self { db }
    }

    pub async fn persist(&self, doc: Document, coll_name: &str) -> Result<String, Error> {
        let coll = self.db.collection(coll_name);
        let result = coll.insert_one(doc, None).await?;

        Ok(result.inserted_id.to_string())
    }
}
