/// HTTP handler for MCP protocol
/// 
/// Provides a simple HTTP POST endpoint for MCP JSON-RPC requests

use std::sync::Arc;
use axum::{
    http::StatusCode,
    response::{IntoResponse, Json as AxumJson, Response},
};
use serde_json::{json, Value};
use tokio::sync::Mutex;
use tracing::{debug, error, warn};

use crate::job_manager::JobManager;
use crate::mcp::server::McpServer;

/// State for MCP HTTP handler
#[derive(Clone)]
pub struct McpState {
    mcp_server: Arc<McpServer>,
}

impl McpState {
    pub fn new(job_manager: Arc<Mutex<JobManager>>) -> Self {
        Self {
            mcp_server: Arc::new(McpServer::new(job_manager)),
        }
    }
}

/// Handle MCP JSON-RPC requests via HTTP POST
/// 
/// Implements basic MCP protocol methods:
/// - initialize: Server initialization
/// - tools/list: List available tools
/// - tools/call: Execute a tool
pub async fn handle_mcp_request(
    state: &McpState,
    payload: Value,
) -> Response {
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
        .ok_or_else(|| json!({
            "code": -32602,
            "message": "Invalid params: missing tool name"
        }))?;
    
    match tool_name {
        "list_jobs" => {
            let jm = state.mcp_server.job_manager().lock().await;
            match jm.list_jobs().await {
                Ok(jobs) => {
                    let jobs_json = serde_json::to_string_pretty(&jobs).map_err(|e| {
                        error!("Failed to serialize jobs: {}", e);
                        json!({
                            "code": -32603,
                            "message": format!("Internal error: {}", e)
                        })
                    })?;
                    
                    Ok(json!({
                        "content": [
                            {
                                "type": "text",
                                "text": jobs_json
                            }
                        ]
                    }))
                }
                Err(e) => {
                    error!("Failed to list jobs: {}", e);
                    Err(json!({
                        "code": -32603,
                        "message": format!("Failed to list jobs: {}", e)
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
