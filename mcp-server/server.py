"""
MCP server: exposes the lakehouse (bronze / silver / gold) to an AI agent.

Claude Desktop connects to this server over stdio; the server in turn issues SQL
to the Iceberg lakehouse through Spark Thrift (spark-thrift:10000). A question
asked in plain language ("what was the best-selling category last month?") is
turned by the agent first into schema discovery via list_tables / describe_table,
then into real SQL via run_query.

Design notes:
- run_query only executes reads (DDL/DML is blocked) -> the agent cannot corrupt
  data by mistake. This enforces a "read-only analytics access" security boundary.
- A connection is opened and closed per query (simple and robust; fine at this scale).
- Results are returned as a plain text table so the agent can read them easily.
"""

import os
import re
import logging

# In MCP stdio mode, stdout is ONLY for protocol messages. If PyHive or other
# libraries print lines to stdout/logs (e.g. "USE default") the protocol can
# break. So we silence logging: set the root logger to WARNING and mute PyHive's
# noise.
logging.basicConfig(level=logging.WARNING)
for noisy in ("pyhive", "thrift", "py4j", "urllib3"):
    logging.getLogger(noisy).setLevel(logging.ERROR)

from mcp.server.fastmcp import FastMCP
from pyhive import hive

THRIFT_HOST = os.environ.get("THRIFT_HOST", "spark-thrift")
THRIFT_PORT = int(os.environ.get("THRIFT_PORT", "10000"))
CATALOG = os.environ.get("LAKEHOUSE_CATALOG", "lakehouse")

mcp = FastMCP("lakehouse")


def _connect():
    return hive.Connection(host=THRIFT_HOST, port=THRIFT_PORT)


def _run(sql: str):
    """Run SQL, return (columns, rows)."""
    conn = _connect()
    try:
        cur = conn.cursor()
        cur.execute(sql)
        cols = [d[0] for d in cur.description] if cur.description else []
        rows = cur.fetchall() if cur.description else []
        return cols, rows
    finally:
        conn.close()


def _format(cols, rows, max_rows: int = 100) -> str:
    """Format the result as a plain, readable text table."""
    if not cols:
        return "(no result)"
    out = [" | ".join(cols), "-" * 40]
    for r in rows[:max_rows]:
        out.append(" | ".join("NULL" if v is None else str(v) for v in r))
    if len(rows) > max_rows:
        out.append(f"... (showing first {max_rows} of {len(rows)} rows)")
    return "\n".join(out)


@mcp.tool()
def list_tables() -> str:
    """
    Lists every namespace (bronze/silver/gold) in the lakehouse and the tables
    inside them. The agent should call this first to know what it can query.
    """
    cols, ns_rows = _run(f"SHOW NAMESPACES IN {CATALOG}")
    lines = []
    for ns in (r[0] for r in ns_rows):
        try:
            _, t_rows = _run(f"SHOW TABLES IN {CATALOG}.{ns}")
            for tr in t_rows:
                # SHOW TABLES: (namespace, tableName, isTemporary)
                tname = tr[1] if len(tr) > 1 else tr[0]
                lines.append(f"{CATALOG}.{ns}.{tname}")
        except Exception as e:
            lines.append(f"{CATALOG}.{ns} (could not read: {e})")
    return "\n".join(lines) if lines else "(no tables found)"


@mcp.tool()
def describe_table(table: str) -> str:
    """
    Returns a table's columns and types. The agent should see the schema before
    querying so it can write correct SQL.

    table: full name, e.g. 'lakehouse.gold.mart_daily_revenue'
    """
    if not re.fullmatch(r"[A-Za-z0-9_.]+", table):
        return "Invalid table name."
    cols, rows = _run(f"DESCRIBE {table}")
    return _format(cols, rows)


@mcp.tool()
def run_query(sql: str) -> str:
    """
    Runs a SELECT query against the lakehouse and returns the result.
    Only SELECT/WITH/SHOW/DESCRIBE is allowed; DDL/DML is rejected.

    sql: full SQL text. Tables must be referenced by full name, e.g.
         'SELECT * FROM lakehouse.gold.mart_sales_by_category'
    """
    cleaned = sql.strip().rstrip(";").strip()
    low = cleaned.lower()
    # Read-only: the first meaningful word must be select/with/show/describe.
    if not re.match(r"^(select|with|show|describe|desc)\b", low):
        return "Rejected: only SELECT/WITH/SHOW/DESCRIBE queries can be run."
    # Extra safety: block dangerous keywords.
    forbidden = r"\b(insert|update|delete|drop|alter|create|truncate|merge|grant|revoke|call)\b"
    if re.search(forbidden, low):
        return "Rejected: the query must be read-only (no DDL/DML)."
    try:
        cols, rows = _run(cleaned)
        return _format(cols, rows)
    except Exception as e:
        return f"Query error: {e}"


if __name__ == "__main__":
    mcp.run()
