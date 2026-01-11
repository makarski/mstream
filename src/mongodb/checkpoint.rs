use mongodb::{Database, bson::doc};
use tokio_stream::StreamExt;

use crate::checkpoint::{Checkpoint, Checkpointer, CheckpointerError};

const MAX_CHECKPOINTS_PER_JOB: u64 = 20;

pub struct MongoDbCheckpointer {
    database: Database,
    coll_name: String,
}

impl MongoDbCheckpointer {
    pub fn new(database: Database, coll_name: String) -> Self {
        MongoDbCheckpointer {
            database,
            coll_name,
        }
    }

    fn collection(&self) -> mongodb::Collection<Checkpoint> {
        self.database.collection(&self.coll_name)
    }
}

#[async_trait::async_trait]
impl Checkpointer for MongoDbCheckpointer {
    async fn load(&self, job_name: &str) -> Result<Checkpoint, CheckpointerError> {
        let filter = doc! { "job_name": job_name };

        let checkpoint = self
            .collection()
            .find_one(filter)
            .sort(doc! { "updated_at": -1 })
            .await?;

        checkpoint.ok_or_else(|| CheckpointerError::NotFound {
            job_name: job_name.to_string(),
        })
    }

    async fn load_all(&self, job_name: &str) -> Result<Vec<Checkpoint>, CheckpointerError> {
        let filter = doc! { "job_name": job_name };

        let mut cursor = self
            .collection()
            .find(filter)
            .sort(doc! { "updated_at": -1 })
            .await?;

        let mut checkpoints = Vec::new();
        while let Some(cp) = cursor.try_next().await? {
            checkpoints.push(cp);
        }

        Ok(checkpoints)
    }

    async fn save(&self, checkpoint: &Checkpoint) -> Result<(), CheckpointerError> {
        let coll = self.collection();
        coll.insert_one(checkpoint).await?;

        let filter = doc! { "job_name": &checkpoint.job_name };
        let mut cursor = coll
            .find(filter.clone())
            .sort(doc! { "updated_at": -1 })
            .skip(MAX_CHECKPOINTS_PER_JOB)
            .limit(1)
            .await?;

        if let Some(oldest_to_keep) = cursor.try_next().await? {
            let delete_filter = doc! {
                "job_name": &checkpoint.job_name,
                "updated_at": { "$lt": oldest_to_keep.updated_at }
            };
            coll.delete_many(delete_filter).await?;
        }

        Ok(())
    }
}
