use mongodb::{
    Collection, Database,
    bson::{doc, to_bson},
};

use crate::job_manager::JobMetadata;

pub struct MongoDBJobStore {
    database: Database,
    coll_name: String,
}

impl MongoDBJobStore {
    pub fn new(database: Database, coll_name: String) -> Self {
        Self {
            database,
            coll_name,
        }
    }

    fn collection(&self) -> Collection<JobMetadata> {
        self.database.collection(&self.coll_name)
    }
}

#[async_trait::async_trait]
impl super::JobLifecycleStorage for MongoDBJobStore {
    async fn list_all(&self) -> anyhow::Result<Vec<JobMetadata>> {
        let mut cursor = self.collection().find(doc! {}).await?;
        let mut jobs = Vec::new();

        while cursor.advance().await? {
            let job_metadata = cursor.deserialize_current()?;
            jobs.push(job_metadata);
        }
        Ok(jobs)
    }

    async fn save(&mut self, metadata: JobMetadata) -> anyhow::Result<()> {
        let filter = doc! { "name": &metadata.name };
        let update_doc = to_bson(&metadata)?;
        let update = doc! { "$set": update_doc };

        self.collection()
            .update_one(filter, update)
            .upsert(true)
            .await?;

        Ok(())
    }

    async fn get(&self, name: &str) -> anyhow::Result<Option<JobMetadata>> {
        let filter = doc! { "name": name };
        let job_metadata = self.collection().find_one(filter).await?;
        Ok(job_metadata)
    }

    async fn get_dependent_jobs(&self, service_name: &str) -> anyhow::Result<Vec<String>> {
        let filter = doc! { "linked_services": service_name };
        let mut cursor = self.collection().find(filter).await?;

        let mut job_names = Vec::new();
        while cursor.advance().await? {
            let job_metadata = cursor.deserialize_current()?;
            job_names.push(job_metadata.name);
        }

        Ok(job_names)
    }
}
