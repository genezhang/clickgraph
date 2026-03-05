"""ClickGraph — embedded graph query engine for Python.

Run Cypher queries over Parquet, Iceberg, Delta Lake and S3 data
without a ClickHouse server.

Quick start::

    import clickgraph

    db = clickgraph.Database("schema.yaml")
    conn = db.connect()
    for row in conn.query("MATCH (u:User) RETURN u.name LIMIT 5"):
        print(row["u.name"])

With S3 credentials::

    db = clickgraph.Database(
        "schema.yaml",
        s3_access_key_id="AKIA...",
        s3_secret_access_key="...",
        s3_region="us-east-1",
    )
"""

from clickgraph._clickgraph import Database, Connection, QueryResult

__all__ = ["Database", "Connection", "QueryResult"]
__version__ = "0.1.0"
