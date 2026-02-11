use async_trait::async_trait;
use mongodb::bson::doc;
use tokio_stream::StreamExt;

use crate::testing::store::{TestSuite, TestSuiteStore, TestSuiteStoreError, TestSuiteSummary};

pub struct MongoDbTestSuiteStore {
    db: mongodb::Database,
    collection_name: String,
}

impl MongoDbTestSuiteStore {
    pub fn new(db: mongodb::Database, collection_name: String) -> Self {
        Self {
            db,
            collection_name,
        }
    }

    fn collection(&self) -> mongodb::Collection<TestSuite> {
        self.db.collection(&self.collection_name)
    }

    pub async fn get(&self, id: &str) -> Result<TestSuite, TestSuiteStoreError> {
        self.collection()
            .find_one(doc! { "id": id })
            .await?
            .ok_or_else(|| TestSuiteStoreError::NotFound(id.to_string()))
    }

    pub async fn list(&self) -> Result<Vec<TestSuiteSummary>, TestSuiteStoreError> {
        let mut cursor = self.collection().find(doc! {}).await?;
        let mut summaries = Vec::new();

        while let Some(suite) = cursor.try_next().await? {
            summaries.push(TestSuiteSummary::from(&suite));
        }

        Ok(summaries)
    }

    pub async fn save(&self, suite: &TestSuite) -> Result<(), TestSuiteStoreError> {
        let filter = doc! { "id": &suite.id };
        let update = doc! {
            "$set": mongodb::bson::to_bson(suite)
                .map_err(|e| TestSuiteStoreError::Other(e.to_string()))?
        };

        self.collection()
            .update_one(filter, update)
            .upsert(true)
            .await?;

        Ok(())
    }

    pub async fn delete(&self, id: &str) -> Result<(), TestSuiteStoreError> {
        self.collection().delete_one(doc! { "id": id }).await?;
        Ok(())
    }
}

#[async_trait]
impl TestSuiteStore for MongoDbTestSuiteStore {
    async fn get(&self, id: &str) -> Result<TestSuite, TestSuiteStoreError> {
        MongoDbTestSuiteStore::get(self, id).await
    }

    async fn list(&self) -> Result<Vec<TestSuiteSummary>, TestSuiteStoreError> {
        MongoDbTestSuiteStore::list(self).await
    }

    async fn save(&self, suite: &TestSuite) -> Result<(), TestSuiteStoreError> {
        MongoDbTestSuiteStore::save(self, suite).await
    }

    async fn delete(&self, id: &str) -> Result<(), TestSuiteStoreError> {
        MongoDbTestSuiteStore::delete(self, id).await
    }
}
