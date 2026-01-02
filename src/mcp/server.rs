//! MCP server core implementation
//!
//! Handles MCP protocol messages and coordinates tool execution
//!
//! This implementation calls the HTTP API instead of directly accessing JobManager
//! to maintain proper separation of concerns and ensure consistent behavior
//! across all client types (REST, MCP, CLI).

use rmcp::{ErrorData as McpError, handler::server::tool::ToolRouter, model::*, tool, tool_router};
use reqwest::Client;
use tracing::{debug, error};

#[derive(Clone)]
pub struct McpServer {
    api_client: Client,
    api_base_url: String,
    #[allow(dead_code)] // Used internally by #[tool_router] macro
    tool_router: ToolRouter<Self>,
}

#[tool_router]
impl McpServer {
    pub fn new(api_base_url: String) -> Self {
        Self {
            api_client: Client::new(),
            api_base_url,
            tool_router: Self::tool_router(),
        }
    }

    /// List all configured mstream jobs with their current status and metadata
    #[tool(
        description = "List all configured mstream jobs with their current status, metadata, and configuration"
    )]
    pub async fn list_jobs(&self) -> Result<CallToolResult, McpError> {
        debug!("Executing list_jobs tool via API");

        let url = format!("{}/jobs", self.api_base_url);
        
        let response = self.api_client
            .get(&url)
            .send()
            .await
            .map_err(|e| {
                error!("API request failed: {}", e);
                McpError::internal_error(format!("Failed to connect to API: {}", e), None)
            })?;

        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            error!("API returned error: {} - {}", status, body);
            return Err(McpError::internal_error(
                format!("API error: {} - {}", status, body),
                None,
            ));
        }

        let jobs: serde_json::Value = response.json().await
            .map_err(|e| {
                error!("Failed to parse API response: {}", e);
                McpError::internal_error(format!("Invalid API response: {}", e), None)
            })?;

        let jobs_json = serde_json::to_string_pretty(&jobs).map_err(|e| {
            error!("Failed to serialize jobs: {}", e);
            McpError::internal_error("Failed to serialize jobs", None)
        })?;

        Ok(CallToolResult::success(vec![Content::text(jobs_json)]))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_tool_router_structure() {
        // Verify the tool router macro creates proper structure
        let tools = McpServer::tool_router().list_all();
        
        assert_eq!(tools.len(), 1, "Should have exactly one tool");
        assert_eq!(tools[0].name, "list_jobs");
        assert!(tools[0].description.is_some());
        assert!(!tools[0].input_schema.is_empty());
    }
}



