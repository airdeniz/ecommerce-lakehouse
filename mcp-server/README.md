# Lakehouse MCP Server

An [MCP](https://modelcontextprotocol.io) server that exposes the lakehouse
(bronze / silver / gold Iceberg tables) to an AI agent such as Claude Desktop.
Ask a question in plain language — *"what was the best-selling category last
month?"* — and the agent discovers the schema and runs the SQL against the
lakehouse through Spark Thrift, then answers in natural language.

## How it fits the pipeline

```
Claude Desktop (agent)
      |  MCP protocol (stdio)
ecom-mcp-server  (this service)
      |  PyHive / Thrift
spark-thrift:10000
      |
Iceberg lakehouse (bronze / silver / gold)
```

The server runs as a container in the same Docker network as the pipeline, so
it reaches Spark Thrift at `spark-thrift:10000`. The container stays idle
(`tail -f /dev/null`); Claude Desktop launches the actual server process on
demand via `docker exec -i`.

## Tools exposed

- **list_tables** — lists every namespace and table in the `lakehouse` catalog.
- **describe_table** — returns a table's columns and types.
- **run_query** — runs a **read-only** query (SELECT / WITH / SHOW / DESCRIBE).
  DDL/DML (INSERT, UPDATE, DELETE, DROP, ALTER, ...) is rejected, so the agent
  can analyse but never modify data.

## Setup

1. Bring the pipeline up (the MCP server builds with it):

   ```bash
   docker compose up -d
   ```

2. Point Claude Desktop at the server. Edit its MCP config file:

   - macOS: `~/Library/Application Support/Claude/claude_desktop_config.json`
   - Windows: `%APPDATA%\Claude\claude_desktop_config.json`

   ```json
   {
     "mcpServers": {
       "lakehouse": {
         "command": "docker",
         "args": ["exec", "-i", "ecom-mcp-server", "python", "server.py"]
       }
     }
   }
   ```

3. Restart Claude Desktop. The `lakehouse` tools appear, and you can ask
   questions like:

   - "List the tables in the lakehouse."
   - "What columns does the daily revenue mart have?"
   - "Which category sold the most? Query the gold layer."

## Design notes

- **Read-only by construction.** `run_query` whitelists read statements and
  blocks DDL/DML keywords, enforcing analytics-only access for the agent.
- **Stateless connections.** Each query opens and closes a Thrift connection —
  simple and robust at this scale.
- **Schema-discovery first.** `list_tables` + `describe_table` let the agent
  learn the structure before writing SQL, so it generates correct queries
  without hard-coded knowledge of the model.
