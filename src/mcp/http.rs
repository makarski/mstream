//! HTTP handler for MCP protocol.

use crate::mcp::server::McpServer;
use axum::{extract::State, http::StatusCode, Json};
use serde_json::{json, Value};
use std::sync::Arc;

/// Shared state for MCP HTTP handler.
#[derive(Clone)]
pub struct McpState {
    pub server: Arc<McpServer>,
}

impl McpState {
    /// Create new MCP state.
    ///
    /// # Arguments
    /// * `api_base_url` - Base URL of the mstream API
    pub fn new(api_base_url: String) -> Self {
        Self {
            server: Arc::new(McpServer::new(api_base_url)),
        }
    }
}

/// Handle MCP JSON-RPC request.
///
/// This handler implements the JSON-RPC 2.0 protocol for MCP.
/// It parses incoming requests, routes them to the appropriate tool,
/// and returns formatted responses.
pub async fn handle_mcp_request_generic(
    State(mcp_state): State<McpState>,
    Json(payload): Json<Value>,
) -> Result<Json<Value>, (StatusCode, String)> {
    // Parse JSON-RPC message
    let id = payload.get("id").cloned().unwrap_or(Value::Null);
    let method = payload
        .get("method")
        .and_then(|v| v.as_str())
        .ok_or_else(|| (StatusCode::BAD_REQUEST, "Missing method".to_string()))?;

    // Route to appropriate handler based on method
    let result = match method {
        "initialize" => handle_initialize(&mcp_state, payload.get("params").cloned()).await,
        "tools/list" => handle_tools_list(&mcp_state).await,
        "tools/call" => handle_tools_call(&mcp_state, payload.get("params").cloned()).await,
        _ => Err(format!("Unknown method: {}", method)),
    };

    // Create JSON-RPC response
    let response = match result {
        Ok(result) => json!({
            "jsonrpc": "2.0",
            "id": id,
            "result": result
        }),
        Err(error) => json!({
            "jsonrpc": "2.0",
            "id": id,
            "error": {
                "code": -32603,
                "message": error
            }
        }),
    };

    Ok(Json(response))
}

/// Handle initialize request.
async fn handle_initialize(_state: &McpState, _params: Option<Value>) -> Result<Value, String> {
    Ok(json!({
        "protocolVersion": "2025-11-25",
        "serverInfo": {
            "name": "mstream-mcp",
            "version": env!("CARGO_PKG_VERSION")
        },
        "capabilities": {
            "tools": {}
        }
    }))
}

/// Handle tools/list request.
async fn handle_tools_list(_state: &McpState) -> Result<Value, String> {
    let tools = vec![
        json!({
            "name": "list_jobs",
            "description": "List all jobs currently managed by mstream. Returns job ID, service name, status, and creation time for each job.",
            "inputSchema": {
                "type": "object",
                "properties": {},
                "required": []
            }
        }),
        json!({
            "name": "create_job",
            "description": "Create a new job for a specified service. The service must already exist in the system.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "service_name": {
                        "type": "string",
                        "description": "Name of the service to create a job for"
                    }
                },
                "required": ["service_name"]
            }
        }),
        json!({
            "name": "stop_job",
            "description": "Stop a currently running job. The job must be in a running state.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "job_id": {
                        "type": "string",
                        "description": "ID of the job to stop"
                    }
                },
                "required": ["job_id"]
            }
        }),
        json!({
            "name": "restart_job",
            "description": "Restart a job. This will stop the job if running and start it again.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "job_id": {
                        "type": "string",
                        "description": "ID of the job to restart"
                    }
                },
                "required": ["job_id"]
            }
        }),
        json!({
            "name": "list_services",
            "description": "List all services configured in mstream. Returns service name and status for each service.",
            "inputSchema": {
                "type": "object",
                "properties": {},
                "required": []
            }
        }),
        json!({
            "name": "create_service",
            "description": "Create a new service with the specified configuration. The configuration must be valid JSON.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "name": {
                        "type": "string",
                        "description": "Name of the service to create"
                    },
                    "config": {
                        "type": "string",
                        "description": "Service configuration as JSON string"
                    }
                },
                "required": ["name", "config"]
            }
        }),
        json!({
            "name": "delete_service",
            "description": "Delete an existing service. All jobs associated with this service must be stopped first.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "name": {
                        "type": "string",
                        "description": "Name of the service to delete"
                    }
                },
                "required": ["name"]
            }
        }),
    ];

    Ok(json!({
        "tools": tools
    }))
}

/// Handle tools/call request.
async fn handle_tools_call(state: &McpState, params: Option<Value>) -> Result<Value, String> {
    let params = params.ok_or_else(|| "Missing params".to_string())?;

    let tool_name = params
        .get("name")
        .and_then(|v| v.as_str())
        .ok_or_else(|| "Missing tool name".to_string())?;

    let default_args = json!({});
    let arguments = params.get("arguments").unwrap_or(&default_args);

    // Call the appropriate tool
    let result = match tool_name {
        "list_jobs" => {
            state.server.list_jobs_impl().await.map_err(|e| e.to_string())
        }
        "create_job" => {
            let service_name = arguments
                .get("service_name")
                .and_then(|v| v.as_str())
                .ok_or_else(|| "Missing service_name".to_string())?
                .to_string();
            state.server.create_job_impl(service_name).await.map_err(|e| e.to_string())
        }
        "stop_job" => {
            let job_id = arguments
                .get("job_id")
                .and_then(|v| v.as_str())
                .ok_or_else(|| "Missing job_id".to_string())?
                .to_string();
            state.server.stop_job_impl(job_id).await.map_err(|e| e.to_string())
        }
        "restart_job" => {
            let job_id = arguments
                .get("job_id")
                .and_then(|v| v.as_str())
                .ok_or_else(|| "Missing job_id".to_string())?
                .to_string();
            state.server.restart_job_impl(job_id).await.map_err(|e| e.to_string())
        }
        "list_services" => {
            state.server.list_services_impl().await.map_err(|e| e.to_string())
        }
        "create_service" => {
            let name = arguments
                .get("name")
                .and_then(|v| v.as_str())
                .ok_or_else(|| "Missing name".to_string())?
                .to_string();
            let config = arguments
                .get("config")
                .and_then(|v| v.as_str())
                .ok_or_else(|| "Missing config".to_string())?
                .to_string();
            state.server.create_service_impl(name, config).await.map_err(|e| e.to_string())
        }
        "delete_service" => {
            let name = arguments
                .get("name")
                .and_then(|v| v.as_str())
                .ok_or_else(|| "Missing name".to_string())?
                .to_string();
            state.server.delete_service_impl(name).await.map_err(|e| e.to_string())
        }
        _ => Err(format!("Unknown tool: {}", tool_name)),
    }?;

    Ok(json!({
        "content": [
            {
                "type": "text",
                "text": result
            }
        ]
    }))
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_mcp_state_creation() {
        let state = McpState::new("http://localhost:8080".to_string());
        assert_eq!(state.server.api_client.base_url, "http://localhost:8080");
    }

    #[test]
    fn test_mcp_state_clone() {
        let state = McpState::new("http://localhost:8080".to_string());
        let cloned = state.clone();
        assert_eq!(
            state.server.api_client.base_url,
            cloned.server.api_client.base_url
        );
    }

    #[tokio::test]
    async fn test_handle_initialize() {
        let state = McpState::new("http://localhost:8080".to_string());
        let result = handle_initialize(&state, None).await;
        assert!(result.is_ok());
        let value = result.unwrap();
        assert_eq!(value["protocolVersion"], "2025-11-25");
        assert_eq!(value["serverInfo"]["name"], "mstream-mcp");
    }

    #[tokio::test]
    async fn test_handle_tools_list() {
        let state = McpState::new("http://localhost:8080".to_string());
        let result = handle_tools_list(&state).await;
        assert!(result.is_ok());
        let value = result.unwrap();
        let tools = value["tools"].as_array();
        assert!(tools.is_some());
        assert_eq!(tools.unwrap().len(), 7);
    }
}

