#!/usr/bin/env python
"""MCP server for DuckLake sessions semantic model."""

from boring_semantic_layer import MCPSemanticModel
from boring_sessions_semantic_model import sessions_sm

# Create MCP server with the semantic model
mcp_server = MCPSemanticModel(
    models={"sessions": sessions_sm},
    name="DuckLake Sessions Analytics"
)

if __name__ == "__main__":
    # Run the server with stdio transport for Claude Desktop integration
    mcp_server.run(transport="stdio")