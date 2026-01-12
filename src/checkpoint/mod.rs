use std::sync::Arc;

use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Checkpoint {
    pub job_name: String,
    pub cursor: Vec<u8>,
    pub updated_at: i64,
}

#[derive(thiserror::Error, Debug)]
pub enum CheckpointerError {
    #[error("checkpoint not found: {job_name}")]
    NotFound { job_name: String },

    #[error("MongoDB error: {0}")]
    MongoDbError(#[from] mongodb::error::Error),
}

pub type DynCheckpointer = Arc<dyn Checkpointer + Send + Sync>;

#[async_trait::async_trait]
pub trait Checkpointer: Send + Sync {
    async fn load(&self, job_name: &str) -> Result<Checkpoint, CheckpointerError>;
    async fn load_all(&self, job_name: &str) -> Result<Vec<Checkpoint>, CheckpointerError>;
    async fn save(&self, checkpoint: &Checkpoint) -> Result<(), CheckpointerError>;
}

pub struct NoopCheckpointer;

impl NoopCheckpointer {
    pub fn new() -> Self {
        NoopCheckpointer
    }
}

#[async_trait::async_trait]
impl Checkpointer for NoopCheckpointer {
    async fn load(&self, _job_name: &str) -> Result<Checkpoint, CheckpointerError> {
        Err(CheckpointerError::NotFound {
            job_name: "noop".to_string(),
        })
    }

    async fn load_all(&self, _job_name: &str) -> Result<Vec<Checkpoint>, CheckpointerError> {
        Ok(vec![])
    }

    async fn save(&self, _checkpoint: &Checkpoint) -> Result<(), CheckpointerError> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn checkpoint_serialization_roundtrip() {
        let checkpoint = Checkpoint {
            job_name: "test-job".to_string(),
            cursor: vec![1, 2, 3, 4, 5],
            updated_at: 1704067200000,
        };

        let serialized = serde_json::to_string(&checkpoint).expect("serialize");
        let deserialized: Checkpoint = serde_json::from_str(&serialized).expect("deserialize");

        assert_eq!(deserialized.job_name, "test-job");
        assert_eq!(deserialized.cursor, vec![1, 2, 3, 4, 5]);
        assert_eq!(deserialized.updated_at, 1704067200000);
    }

    #[test]
    fn checkpoint_bson_serialization_roundtrip() {
        let checkpoint = Checkpoint {
            job_name: "kafka-consumer".to_string(),
            cursor: vec![10, 20, 30],
            updated_at: 1704153600000,
        };

        let serialized = mongodb::bson::to_vec(&checkpoint).expect("bson serialize");
        let deserialized: Checkpoint =
            mongodb::bson::from_slice(&serialized).expect("bson deserialize");

        assert_eq!(deserialized.job_name, "kafka-consumer");
        assert_eq!(deserialized.cursor, vec![10, 20, 30]);
        assert_eq!(deserialized.updated_at, 1704153600000);
    }

    #[test]
    fn checkpointer_error_not_found_display() {
        let err = CheckpointerError::NotFound {
            job_name: "missing-job".to_string(),
        };
        assert_eq!(err.to_string(), "checkpoint not found: missing-job");
    }

    #[tokio::test]
    async fn noop_checkpointer_load_returns_not_found() {
        let checkpointer = NoopCheckpointer::new();
        let result = checkpointer.load("any-job").await;

        assert!(result.is_err());
        match result.unwrap_err() {
            CheckpointerError::NotFound { job_name } => {
                assert_eq!(job_name, "noop");
            }
            _ => panic!("expected NotFound error"),
        }
    }

    #[tokio::test]
    async fn noop_checkpointer_save_returns_ok() {
        let checkpointer = NoopCheckpointer::new();
        let checkpoint = Checkpoint {
            job_name: "test".to_string(),
            cursor: vec![],
            updated_at: 0,
        };

        let result = checkpointer.save(&checkpoint).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn noop_checkpointer_load_all_returns_empty() {
        let checkpointer = NoopCheckpointer::new();
        let result = checkpointer.load_all("any-job").await;

        assert!(result.is_ok());
        assert!(result.unwrap().is_empty());
    }
}
