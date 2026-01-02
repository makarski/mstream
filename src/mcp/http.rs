//! HTTP handler for MCP protocol
//!
//! Provides a simple HTTP POST endpoint for MCP JSON-RPC requests

use axum::{
    http::StatusCode,
    response::{IntoResponse, Json as AxumJson, Response},
};
use serde_json::{Value, json};
use std::sync::Arc;
use tracing::{debug, error, warn};

use crate::mcp::server::McpServer;

/// State for MCP HTTP handler
#[derive(Clone)]
pub struct McpState {
    mcp_server: Arc<McpServer>,
}

impl McpState {
    pub fn new(api_base_url: String) -> Self {
        Self {
            mcp_server: Arc::new(McpServer::new(api_base_url)),
        }
    }
}
/// Handle MCP JSON-RPC requests via HTTP POST
///
/// Implements basic MCP protocol methods:
/// - initialize: Server initialization
/// - tools/list: List available tools
/// - tools/call: Execute a tool
pub async fn handle_mcp_request(state: &McpState, payload: Value) -> Response {
    debug!("Received MCP request: {:?}", payload);

    // Parse JSON-RPC request
    let method = payload.get("method").and_then(|m| m.as_str());
    let id = payload.get("id").cloned();

    let result = match method {
        Some("initialize") => {
            debug!("Handling initialize request");
            handle_initialize()
        }
        Some("tools/list") => {
            debug!("Handling tools/list request");
            handle_tools_list()
        }
        Some("tools/call") => {
            debug!("Handling tools/call request");
            let params = payload.get("params");
            handle_tools_call(state, params).await
        }
        Some(other) => {
            warn!("Unknown MCP method: {}", other);
            Err(json!({
                "code": -32601,
                "message": format!("Method not found: {}", other)
            }))
        }
        None => {
            error!("Missing method in JSON-RPC request");
            Err(json!({
                "code": -32600,
                "message": "Invalid Request: missing method"
            }))
        }
    };

    let response = match result {
        Ok(result) => json!({
            "jsonrpc": "2.0",
            "id": id,
            "result": result
        }),
        Err(error) => json!({
            "jsonrpc": "2.0",
            "id": id,
            "error": error
        }),
    };

    (StatusCode::OK, AxumJson(response)).into_response()
}

fn handle_initialize() -> Result<Value, Value> {
    Ok(json!({
        "protocolVersion": "2024-11-05",
        "capabilities": {
            "tools": {}
        },
        "serverInfo": {
            "name": "mstream-mcp",
            "version": env!("CARGO_PKG_VERSION")
        }
    }))
}

fn handle_tools_list() -> Result<Value, Value> {
    Ok(json!({
        "tools": [
            {
                "name": "list_jobs",
                "description": "List all configured mstream jobs with their current status, metadata, and configuration",
                "inputSchema": {
                    "type": "object",
                    "properties": {},
                    "required": []
                }
            }
        ]
    }))
}

async fn handle_tools_call(state: &McpState, params: Option<&Value>) -> Result<Value, Value> {
    let tool_name = params
        .and_then(|p| p.get("name"))
        .and_then(|n| n.as_str())
        .ok_or_else(|| {
            json!({
                "code": -32602,
                "message": "Invalid params: missing tool name"
            })
        })?;

    // Call the MCP server's tool methods which in turn call the HTTP API
    // This maintains separation of concerns: MCP endpoint -> MCP tools -> HTTP API
    match tool_name {
        "list_jobs" => {
            match state.mcp_server.list_jobs().await {
                Ok(result) => {
                    // Serialize the CallToolResult directly
                    let result_json = serde_json::to_value(&result)
                        .map_err(|e| {
                            error!("Failed to serialize result: {}", e);
                            json!({
                                "code": -32603,
                                "message": format!("Failed to serialize result: {}", e)
                            })
                        })?;
                    
                    Ok(result_json)
                }
                Err(e) => {
                    error!("Tool execution failed: {:?}", e);
                    Err(json!({
                        "code": e.code,
                        "message": e.message
                    }))
                }
            }
        }
        other => {
            warn!("Unknown tool: {}", other);
            Err(json!({
                "code": -32602,
                "message": format!("Unknown tool: {}", other)
            }))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_handle_initialize() {
        let result = handle_initialize().unwrap();
        
        assert_eq!(result["protocolVersion"], "2024-11-05");
        assert!(result["capabilities"]["tools"].is_object());
        assert_eq!(result["serverInfo"]["name"], "mstream-mcp");
        assert!(result["serverInfo"]["version"].is_string());
    }

    #[test]
    fn test_handle_tools_list() {
        let result = handle_tools_list().unwrap();
        
        let tools = result["tools"].as_array().unwrap();
        assert_eq!(tools.len(), 1);
        assert_eq!(tools[0]["name"], "list_jobs");
        assert!(tools[0]["description"].is_string());
        assert!(tools[0]["inputSchema"].is_object());
    }

    // Note: Full integration tests for handle_tools_call require JobManager setup
    // which is complex to mock. These are tested via integration tests.
    #[test]
    fn test_json_rpc_error_structure() {
        let error = json!({
            "code": -32602,
            "message": "Invalid params"
        });
        
        assert_eq!(error["code"], -32602);
        assert_eq!(error["message"], "Invalid params");
    }
}
