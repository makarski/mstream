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

    #[error("Resource '{0}' not found in service '{1}'")]
    ResourceNotFound(String, String),

    #[error("Invalid request: {0}")]
    InvalidRequest(String),

    #[error("Internal error: {0}")]
    InternalError(String),

    #[error("Service '{0}' is in use by jobs: {1}")]
    ServiceInUse(String, String),

    #[error(transparent)]
    Anyhow(#[from] anyhow::Error),
}

pub type Result<T> = std::result::Result<T, JobManagerError>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn job_not_found_display() {
        let err = JobManagerError::JobNotFound("my-job".to_string());
        assert_eq!(err.to_string(), "Job 'my-job' not found");
    }

    #[test]
    fn job_already_exists_display() {
        let err = JobManagerError::JobAlreadyExists("existing-job".to_string());
        assert_eq!(err.to_string(), "Job 'existing-job' already exists");
    }

    #[test]
    fn service_not_found_display() {
        let err = JobManagerError::ServiceNotFound("kafka-service".to_string());
        assert_eq!(err.to_string(), "Service 'kafka-service' not found");
    }

    #[test]
    fn service_already_exists_display() {
        let err = JobManagerError::ServiceAlreadyExists("mongo-service".to_string());
        assert_eq!(err.to_string(), "Service 'mongo-service' already exists");
    }

    #[test]
    fn resource_not_found_display() {
        let err =
            JobManagerError::ResourceNotFound("script.rhai".to_string(), "my-udf".to_string());
        assert_eq!(
            err.to_string(),
            "Resource 'script.rhai' not found in service 'my-udf'"
        );
    }

    #[test]
    fn invalid_request_display() {
        let err = JobManagerError::InvalidRequest("service 'foo' is not a UDF service".to_string());
        assert_eq!(
            err.to_string(),
            "Invalid request: service 'foo' is not a UDF service"
        );
    }

    #[test]
    fn internal_error_display() {
        let err = JobManagerError::InternalError("connection timeout".to_string());
        assert_eq!(err.to_string(), "Internal error: connection timeout");
    }

    #[test]
    fn service_in_use_display() {
        let err =
            JobManagerError::ServiceInUse("shared-mongo".to_string(), "job-a, job-b".to_string());
        assert_eq!(
            err.to_string(),
            "Service 'shared-mongo' is in use by jobs: job-a, job-b"
        );
    }

    #[test]
    fn from_anyhow_error() {
        let anyhow_err = anyhow::anyhow!("something went wrong");
        let job_err: JobManagerError = anyhow_err.into();

        match job_err {
            JobManagerError::Anyhow(e) => {
                assert_eq!(e.to_string(), "something went wrong");
            }
            _ => panic!("expected Anyhow variant"),
        }
    }
}
