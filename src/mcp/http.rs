//! HTTP handler for MCP protocol.

use crate::mcp::server::McpServer;
use axum::{extract::State, http::StatusCode, Json};
use serde_json::Value;
use std::sync::Arc;

/// Shared state for MCP HTTP handler.
#[derive(Clone)]
pub struct McpState {
    server: Arc<McpServer>,
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
pub async fn handle_mcp_request(
    State(_state): State<McpState>,
    Json(_payload): Json<Value>,
) -> Result<Json<Value>, (StatusCode, String)> {
    // Placeholder implementation
    Ok(Json(serde_json::json!({
        "jsonrpc": "2.0",
        "id": 1,
        "result": {
            "protocolVersion": "2024-11-05",
            "serverInfo": {
                "name": "mstream-mcp",
                "version": env!("CARGO_PKG_VERSION")
            },
            "capabilities": {
                "tools": {}
            }
        }
    })))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_mcp_state_creation() {
        let state = McpState::new("http://localhost:8080".to_string());
        assert_eq!(state.server.api_client.base_url, "http://localhost:8080");
    }
}
