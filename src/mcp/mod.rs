//! MCP (Model Context Protocol) server implementation.
//!
//! This module provides MCP protocol support for mstream, allowing external
//! clients like Claude Desktop to interact with the mstream API through
//! standardized MCP tools.
//!
//! Architecture:
//! - `server.rs`: MCP tool definitions using rmcp macros
//! - `http.rs`: HTTP JSON-RPC handler for MCP protocol
//! - `client.rs`: HTTP client wrapper for calling mstream API
//!
//! All MCP operations are fully decoupled from business logic and only
//! interact with the HTTP API endpoints.

pub mod client;
pub mod http;
pub mod server;

pub use client::ApiClient;
pub use http::{handle_mcp_request_generic as handle_mcp_request, McpState};
pub use server::McpServer;
