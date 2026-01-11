use thiserror::Error;

#[derive(Debug, Error)]
pub enum JobManagerError {
    #[error("Job '{0}' not found")]
    JobNotFound(String),

    #[error("Job '{0}' already exists")]
    JobAlreadyExists(String),

    #[error("Service '{0}' not found")]
    ServiceNotFound(String),

    #[error("Service '{0}' already exists")]
    ServiceAlreadyExists(String),

    #[error("Internal error: {0}")]
    InternalError(String),

    #[error("Service '{0}' is in use by jobs: {1}")]
    ServiceInUse(String, String),

    #[error(transparent)]
    Anyhow(#[from] anyhow::Error),
}

pub type Result<T> = std::result::Result<T, JobManagerError>;
