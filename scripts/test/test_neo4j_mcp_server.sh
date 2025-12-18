#!/bin/bash
# Test Neo4j MCP Server with ClickGraph's Bolt Protocol
# This script tests if we can use Neo4j's MCP server with ClickGraph

set -e

BOLT_PORT=7688

echo "========================================"
echo "Testing Neo4j MCP Server with ClickGraph"
echo "========================================"
echo ""

# Check if the isolated ClickGraph instance is running
if ! curl -s "http://localhost:8081/health" > /dev/null 2>&1; then
    echo "Error: ClickGraph test instance not running on port 8081"
    echo "Start it with: ./scripts/test/test_mcp_compatibility.sh"
    exit 1
fi

echo "✅ ClickGraph test instance is running"
echo ""

# Test Neo4j MCP server
echo "Testing @modelcontextprotocol/server-neo4j..."
echo "Connection: bolt://localhost:$BOLT_PORT"
echo ""

# Create a simple test to see if MCP server starts
timeout 5s npx @modelcontextprotocol/server-neo4j bolt://localhost:$BOLT_PORT <<EOF || true
{"jsonrpc":"2.0","id":1,"method":"tools/list"}
EOF

echo ""
echo "========================================"
echo "Manual Testing Instructions"
echo "========================================"
echo ""
echo "To test with Claude Desktop, add this to your MCP config:"
echo ""
echo '{'
echo '  "mcpServers": {'
echo '    "clickgraph-test": {'
echo '      "command": "npx",'
echo '      "args": ['
echo '        "@modelcontextprotocol/server-neo4j",'
echo '        "bolt://localhost:7688"'
echo '      ],'
echo '      "env": {'
echo '        "NEO4J_USERNAME": "neo4j",'
echo '        "NEO4J_PASSWORD": "password"'
echo '      }'
echo '    }'
echo '  }'
echo '}'
echo ""
echo "Then ask Claude: 'Query the clickgraph-test database for users'"
