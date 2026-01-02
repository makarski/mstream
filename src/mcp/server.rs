//! MCP server with tool definitions.

use crate::mcp::client::ApiClient;

/// MCP server providing tools for mstream operations.
pub struct McpServer {
    pub api_client: ApiClient,
}

impl McpServer {
    /// Create a new MCP server.
    ///
    /// # Arguments
    /// * `api_base_url` - Base URL of the mstream API (e.g., "http://localhost:8080")
    pub fn new(api_base_url: String) -> Self {
        Self {
            api_client: ApiClient::new(api_base_url),
        }
    }

    // ---------------------------------------------------------------
    // Job Management Tools
    // ---------------------------------------------------------------

    /// List all jobs in the system.
    pub async fn list_jobs_impl(&self) -> anyhow::Result<String> {
        let jobs = self.api_client.list_jobs().await?;
        Ok(serde_json::to_string_pretty(&jobs)?)
    }

    /// Create a new job for a service.
    pub async fn create_job_impl(&self, service_name: String) -> anyhow::Result<String> {
        let job = self.api_client.create_job(service_name).await?;
        Ok(serde_json::to_string_pretty(&job)?)
    }

    /// Stop a running job.
    pub async fn stop_job_impl(&self, job_id: String) -> anyhow::Result<String> {
        self.api_client.stop_job(job_id.clone()).await?;
        Ok(format!("Job {} stopped successfully", job_id))
    }

    /// Restart a job.
    pub async fn restart_job_impl(&self, job_id: String) -> anyhow::Result<String> {
        self.api_client.restart_job(job_id.clone()).await?;
        Ok(format!("Job {} restarted successfully", job_id))
    }

    // ---------------------------------------------------------------
    // Service Management Tools
    // ---------------------------------------------------------------

    /// List all services in the system.
    pub async fn list_services_impl(&self) -> anyhow::Result<String> {
        let services = self.api_client.list_services().await?;
        Ok(serde_json::to_string_pretty(&services)?)
    }

    /// Create a new service.
    pub async fn create_service_impl(&self, name: String, config: String) -> anyhow::Result<String> {
        let config_json: serde_json::Value = serde_json::from_str(&config)?;
        let service = self.api_client.create_service(name, config_json).await?;
        Ok(serde_json::to_string_pretty(&service)?)
    }

    /// Delete a service.
    pub async fn delete_service_impl(&self, name: String) -> anyhow::Result<String> {
        self.api_client.delete_service(name.clone()).await?;
        Ok(format!("Service {} deleted successfully", name))
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_mcp_server_creation() {
        let server = McpServer::new("http://localhost:8080".to_string());
        assert_eq!(server.api_client.base_url, "http://localhost:8080");
    }
}
