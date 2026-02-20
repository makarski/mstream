use async_trait::async_trait;
use mongodb::bson::doc;
use tokio_stream::StreamExt;

use crate::workspace::{Workspace, WorkspaceStore, WorkspaceStoreError, WorkspaceSummary};

pub struct MongoDbWorkspaceStore {
    db: mongodb::Database,
    collection_name: String,
}

impl MongoDbWorkspaceStore {
    pub fn new(db: mongodb::Database, collection_name: String) -> Self {
        Self {
            db,
            collection_name,
        }
    }

    fn collection(&self) -> mongodb::Collection<Workspace> {
        self.db.collection(&self.collection_name)
    }

    pub async fn get(&self, id: &str) -> Result<Workspace, WorkspaceStoreError> {
        self.collection()
            .find_one(doc! { "id": id })
            .await?
            .ok_or_else(|| WorkspaceStoreError::NotFound(id.to_string()))
    }

    pub async fn list(&self) -> Result<Vec<WorkspaceSummary>, WorkspaceStoreError> {
        let mut cursor = self.collection().find(doc! {}).await?;
        let mut summaries = Vec::new();

        while let Some(ws) = cursor.try_next().await? {
            summaries.push(WorkspaceSummary::from(&ws));
        }

        Ok(summaries)
    }

    pub async fn save(&self, workspace: &Workspace) -> Result<(), WorkspaceStoreError> {
        let filter = doc! { "id": &workspace.id };
        let update = doc! {
            "$set": mongodb::bson::to_bson(workspace)
                .map_err(|e| WorkspaceStoreError::Other(e.to_string()))?
        };

        self.collection()
            .update_one(filter, update)
            .upsert(true)
            .await?;

        Ok(())
    }

    pub async fn delete(&self, id: &str) -> Result<(), WorkspaceStoreError> {
        self.collection().delete_one(doc! { "id": id }).await?;
        Ok(())
    }
}

#[async_trait]
impl WorkspaceStore for MongoDbWorkspaceStore {
    async fn get(&self, id: &str) -> Result<Workspace, WorkspaceStoreError> {
        MongoDbWorkspaceStore::get(self, id).await
    }

    async fn list(&self) -> Result<Vec<WorkspaceSummary>, WorkspaceStoreError> {
        MongoDbWorkspaceStore::list(self).await
    }

    async fn save(&self, workspace: &Workspace) -> Result<(), WorkspaceStoreError> {
        MongoDbWorkspaceStore::save(self, workspace).await
    }

    async fn delete(&self, id: &str) -> Result<(), WorkspaceStoreError> {
        MongoDbWorkspaceStore::delete(self, id).await
    }
}
