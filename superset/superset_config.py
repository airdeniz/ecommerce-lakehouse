import os
SQLALCHEMY_DATABASE_URI = os.environ.get("SQLALCHEMY_DATABASE_URI")

# --- PyHive / Spark Thrift compatibility patch -----------------------------
# Superset queries Spark Thrift through PyHive's Hive dialect. Spark's
# `SHOW TABLES IN <schema>` returns THREE columns (namespace, tableName,
# isTemporary), whereas PyHive's get_table_names assumes Hive's single-column
# output and takes row[0] — which for Spark is the namespace, not the table.
# The effect: the table dropdown shows the schema name (e.g. "silver") instead
# of the real tables (core_orders, ...). Pick the tableName column when present.
try:
    from pyhive.sqlalchemy_hive import HiveDialect
    from sqlalchemy import text

    def _spark_get_table_names(self, connection, schema=None, **kw):
        query = "SHOW TABLES"
        if schema:
            query += " IN " + self.identifier_preparer.quote_identifier(schema)
        rows = connection.execute(text(query)).fetchall()
        # Spark: (namespace, tableName, isTemporary); Hive: (tab_name,)
        return [row[1] if len(row) > 1 else row[0] for row in rows]

    HiveDialect.get_table_names = _spark_get_table_names
except Exception:
    # Never let a patch failure stop Superset from booting.
    pass
