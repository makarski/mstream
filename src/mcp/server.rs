use rmcp::{ErrorData as McpError, handler::server::tool::ToolRouter, model::*, tool, tool_router};
/// MCP server core implementation
///
/// Handles MCP protocol messages and coordinates tool execution
use std::sync::Arc;
use tokio::sync::Mutex;
use tracing::{debug, error};

use crate::job_manager::JobManager;

#[derive(Clone)]
pub struct McpServer {
    job_manager: Arc<Mutex<JobManager>>,
    tool_router: ToolRouter<Self>,
}

#[tool_router]
impl McpServer {
    pub fn new(job_manager: Arc<Mutex<JobManager>>) -> Self {
        Self {
            job_manager,
            tool_router: Self::tool_router(),
        }
    }

    /// Get access to the job manager for direct queries
    pub fn job_manager(&self) -> &Arc<Mutex<JobManager>> {
        &self.job_manager
    }

    /// List all configured mstream jobs with their current status and metadata
    #[tool(
        description = "List all configured mstream jobs with their current status, metadata, and configuration"
    )]
    async fn list_jobs(&self) -> Result<CallToolResult, McpError> {
        debug!("Executing list_jobs tool");

        let jm = self.job_manager.lock().await;

        match jm.list_jobs().await {
            Ok(jobs) => {
                let jobs_json = serde_json::to_string_pretty(&jobs).map_err(|e| {
                    error!("Failed to serialize jobs: {}", e);
                    McpError::internal_error("Failed to serialize jobs", None)
                })?;

                Ok(CallToolResult::success(vec![Content::text(jobs_json)]))
            }
            Err(e) => {
                error!("Failed to list jobs: {}", e);
                Err(McpError::internal_error(
                    format!("Failed to list jobs: {}", e),
                    None,
                ))
            }
        }
    }
}
