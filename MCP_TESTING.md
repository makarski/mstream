# Testing MCP with VS Code

## Architecture Overview

This implementation uses a **minimal HTTP JSON-RPC endpoint** approach rather than rmcp's full service integration:

- **Tool Discovery**: Automated via `#[tool_router]` and `#[tool]` macros
- **Tool Listing**: Uses `router().list_all()` for automatic tool enumeration
- **Tool Execution**: Manual dispatch in HTTP handler

### Why Not Use `router().call()`?

The `rmcp` library is designed for complete MCP service integration using `serve_server()` with transport layers (stdio, SSE, etc.). This requires:

1. `RequestContext<RoleServer>` - created by the service layer during transport initialization
2. Full transport implementation (stdin/stdout, HTTP SSE, etc.)
3. Additional service lifecycle management

Our design philosophy:
- ✅ **Minimal**: Simple HTTP endpoint without transport overhead
- ✅ **Pragmatic**: Manual dispatch is straightforward for a single tool
- ✅ **Flexible**: Easy to add more tools or migrate to full service layer later

If you need many tools, consider migrating to `serve_server()` with an HTTP transport.

## Prerequisites

1. **VS Code with GitHub Copilot** installed and active
2. **mstream server** running locally

## Setup Instructions

### 1. Start mstream Server

First, make sure your mstream server is running:

```bash
# From the mstream project root
./target/release/mstream --config mstream-config.toml
```

The API server should start on port 8787.

### 2. Configure VS Code for MCP

VS Code's GitHub Copilot should automatically detect the `.vscode/mcp-settings.json` file in this project.

If not, you can manually configure it in your VS Code settings:

1. Open VS Code Settings (Cmd+, on Mac, Ctrl+, on Windows/Linux)
2. Search for "MCP"
3. Look for "GitHub Copilot > MCP: Servers"
4. Add the mstream server configuration:

```json
{
  "github.copilot.advanced": {
    "mcp": {
      "servers": {
        "mstream": {
          "type": "http",
          "url": "http://localhost:8787/mcp"
        }
      }
    }
  }
}
```

### 3. Test the Integration

Once configured, you can test the MCP integration:

#### In VS Code Chat:

1. Open GitHub Copilot Chat (Cmd+Shift+I or click the chat icon)
2. Try these prompts:

```
@mstream list all jobs

Show me the current mstream job statuses

What jobs are running in mstream?
```

#### Manual Testing with curl:

You can also test the endpoint directly:

```bash
# Initialize
curl -X POST http://localhost:8787/mcp \
  -H "Content-Type: application/json" \
  -d '{
    "jsonrpc": "2.0",
    "id": 1,
    "method": "initialize",
    "params": {}
  }' | jq .

# List available tools
curl -X POST http://localhost:8787/mcp \
  -H "Content-Type: application/json" \
  -d '{
    "jsonrpc": "2.0",
    "id": 2,
    "method": "tools/list",
    "params": {}
  }' | jq .

# Call list_jobs tool
curl -X POST http://localhost:8787/mcp \
  -H "Content-Type: application/json" \
  -d '{
    "jsonrpc": "2.0",
    "id": 3,
    "method": "tools/call",
    "params": {
      "name": "list_jobs",
      "arguments": {}
    }
  }' | jq .
```

## Expected Responses

### Initialize Response:
```json
{
  "jsonrpc": "2.0",
  "id": 1,
  "result": {
    "protocolVersion": "2024-11-05",
    "capabilities": {
      "tools": {}
    },
    "serverInfo": {
      "name": "mstream-mcp",
      "version": "0.26.0"
    }
  }
}
```

### Tools List Response:
```json
{
  "jsonrpc": "2.0",
  "id": 2,
  "result": {
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
  }
}
```

### List Jobs Response:
```json
{
  "jsonrpc": "2.0",
  "id": 3,
  "result": {
    "content": [
      {
        "type": "text",
        "text": "[...job data in JSON format...]"
      }
    ]
  }
}
```

## Troubleshooting

### MCP endpoint not responding:
- Check if mstream is running: `ps aux | grep mstream`
- Verify the port: `curl http://localhost:8787/jobs`
- Check server logs for errors

### VS Code not detecting MCP server:
- Restart VS Code after creating the config
- Check GitHub Copilot extension is enabled
- Verify the `.vscode/mcp-settings.json` file exists
- Try adding the configuration to User settings instead of Workspace settings

### Connection refused:
- Ensure mstream is running on port 8787
- Check firewall settings
- Try accessing `http://localhost:8787/mcp` in a browser or with curl first

## Alternative: Testing with Claude Desktop

You can also test with Claude Desktop app:

1. Open Claude Desktop settings
2. Find the "Developer" section
3. Add MCP server configuration:

```json
{
  "mcpServers": {
    "mstream": {
      "url": "http://localhost:8787/mcp"
    }
  }
}
```

4. Restart Claude Desktop
5. Try asking: "List all mstream jobs"

## Notes

- The MCP endpoint is always available when mstream is running
- No additional configuration needed in mstream-config.toml
- The endpoint uses standard JSON-RPC 2.0 protocol
- All responses follow MCP protocol specification
