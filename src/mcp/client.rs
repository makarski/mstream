//! HTTP client for calling mstream API endpoints.

use anyhow::{Context, Result};
use reqwest::Client;
use serde::{Deserialize, Serialize};

/// HTTP client wrapper for mstream API.
#[derive(Clone)]
pub struct ApiClient {
    pub client: Client,
    pub base_url: String,
}

// ============================================================================
// Request/Response Types
// ============================================================================

#[derive(Debug, Serialize)]
pub struct CreateJobRequest {
    pub service_name: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Job {
    pub id: String,
    pub service_name: String,
    pub status: String,
    pub created_at: String,
}

#[derive(Debug, Deserialize)]
pub struct JobsResponse {
    pub jobs: Vec<Job>,
}

#[derive(Debug, Serialize)]
pub struct CreateServiceRequest {
    pub name: String,
    pub config: serde_json::Value,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Service {
    pub name: String,
    pub status: String,
}

#[derive(Debug, Deserialize)]
pub struct ServicesResponse {
    pub services: Vec<Service>,
}

#[derive(Debug, Deserialize)]
pub struct ApiErrorResponse {
    pub error: String,
}

// ============================================================================
// ApiClient Implementation
// ============================================================================

impl ApiClient {
    /// Create a new API client.
    ///
    /// # Arguments
    /// * `base_url` - Base URL of the mstream API (e.g., "http://localhost:8080")
    pub fn new(base_url: String) -> Self {
        Self {
            client: Client::new(),
            base_url,
        }
    }

    // ------------------------------------------------------------------------
    // Job Operations
    // ------------------------------------------------------------------------

    /// List all jobs.
    pub async fn list_jobs(&self) -> Result<Vec<Job>> {
        let url = format!("{}/jobs", self.base_url);
        let response = self
            .client
            .get(&url)
            .send()
            .await
            .context("Failed to send GET /jobs request")?;

        if !response.status().is_success() {
            let status = response.status();
            let error_body = response
                .json::<ApiErrorResponse>()
                .await
                .unwrap_or_else(|_| ApiErrorResponse {
                    error: "Unknown error".to_string(),
                });
            anyhow::bail!("API error {}: {}", status, error_body.error);
        }

        let jobs_response = response
            .json::<JobsResponse>()
            .await
            .context("Failed to parse jobs response")?;

        Ok(jobs_response.jobs)
    }

    /// Create a new job.
    ///
    /// # Arguments
    /// * `service_name` - Name of the service to create a job for
    pub async fn create_job(&self, service_name: String) -> Result<Job> {
        let url = format!("{}/jobs", self.base_url);
        let request = CreateJobRequest { service_name };

        let response = self
            .client
            .post(&url)
            .json(&request)
            .send()
            .await
            .context("Failed to send POST /jobs request")?;

        if !response.status().is_success() {
            let status = response.status();
            let error_body = response
                .json::<ApiErrorResponse>()
                .await
                .unwrap_or_else(|_| ApiErrorResponse {
                    error: "Unknown error".to_string(),
                });
            anyhow::bail!("API error {}: {}", status, error_body.error);
        }

        let job = response
            .json::<Job>()
            .await
            .context("Failed to parse job response")?;

        Ok(job)
    }

    /// Stop a running job.
    ///
    /// # Arguments
    /// * `job_id` - ID of the job to stop
    pub async fn stop_job(&self, job_id: String) -> Result<()> {
        let url = format!("{}/jobs/{}/stop", self.base_url, job_id);

        let response = self
            .client
            .post(&url)
            .send()
            .await
            .context("Failed to send POST /jobs/{id}/stop request")?;

        if !response.status().is_success() {
            let status = response.status();
            let error_body = response
                .json::<ApiErrorResponse>()
                .await
                .unwrap_or_else(|_| ApiErrorResponse {
                    error: "Unknown error".to_string(),
                });
            anyhow::bail!("API error {}: {}", status, error_body.error);
        }

        Ok(())
    }

    /// Restart a job.
    ///
    /// # Arguments
    /// * `job_id` - ID of the job to restart
    pub async fn restart_job(&self, job_id: String) -> Result<()> {
        let url = format!("{}/jobs/{}/restart", self.base_url, job_id);

        let response = self
            .client
            .post(&url)
            .send()
            .await
            .context("Failed to send POST /jobs/{id}/restart request")?;

        if !response.status().is_success() {
            let status = response.status();
            let error_body = response
                .json::<ApiErrorResponse>()
                .await
                .unwrap_or_else(|_| ApiErrorResponse {
                    error: "Unknown error".to_string(),
                });
            anyhow::bail!("API error {}: {}", status, error_body.error);
        }

        Ok(())
    }

    // ------------------------------------------------------------------------
    // Service Operations
    // ------------------------------------------------------------------------

    /// List all services.
    pub async fn list_services(&self) -> Result<Vec<Service>> {
        let url = format!("{}/services", self.base_url);
        let response = self
            .client
            .get(&url)
            .send()
            .await
            .context("Failed to send GET /services request")?;

        if !response.status().is_success() {
            let status = response.status();
            let error_body = response
                .json::<ApiErrorResponse>()
                .await
                .unwrap_or_else(|_| ApiErrorResponse {
                    error: "Unknown error".to_string(),
                });
            anyhow::bail!("API error {}: {}", status, error_body.error);
        }

        let services_response = response
            .json::<ServicesResponse>()
            .await
            .context("Failed to parse services response")?;

        Ok(services_response.services)
    }

    /// Create a new service.
    ///
    /// # Arguments
    /// * `name` - Name of the service
    /// * `config` - Service configuration as JSON
    pub async fn create_service(&self, name: String, config: serde_json::Value) -> Result<Service> {
        let url = format!("{}/services", self.base_url);
        let request = CreateServiceRequest { name, config };

        let response = self
            .client
            .post(&url)
            .json(&request)
            .send()
            .await
            .context("Failed to send POST /services request")?;

        if !response.status().is_success() {
            let status = response.status();
            let error_body = response
                .json::<ApiErrorResponse>()
                .await
                .unwrap_or_else(|_| ApiErrorResponse {
                    error: "Unknown error".to_string(),
                });
            anyhow::bail!("API error {}: {}", status, error_body.error);
        }

        let service = response
            .json::<Service>()
            .await
            .context("Failed to parse service response")?;

        Ok(service)
    }

    /// Delete a service.
    ///
    /// # Arguments
    /// * `name` - Name of the service to delete
    pub async fn delete_service(&self, name: String) -> Result<()> {
        let url = format!("{}/services/{}", self.base_url, name);

        let response = self
            .client
            .delete(&url)
            .send()
            .await
            .context("Failed to send DELETE /services/{name} request")?;

        if !response.status().is_success() {
            let status = response.status();
            let error_body = response
                .json::<ApiErrorResponse>()
                .await
                .unwrap_or_else(|_| ApiErrorResponse {
                    error: "Unknown error".to_string(),
                });
            anyhow::bail!("API error {}: {}", status, error_body.error);
        }

        Ok(())
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_api_client_creation() {
        let client = ApiClient::new("http://localhost:8080".to_string());
        assert_eq!(client.base_url, "http://localhost:8080");
    }

    #[test]
    fn test_api_client_clone() {
        let client = ApiClient::new("http://localhost:8080".to_string());
        let cloned = client.clone();
        assert_eq!(client.base_url, cloned.base_url);
    }
}
