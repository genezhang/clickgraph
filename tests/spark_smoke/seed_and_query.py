"""Container-side helper for the Spark smoke tests.

Runs inside `deltaio/delta-docker:4.1.0`. Seeds the LDBC SNB mini dataset
as Delta tables under /tmp/ldbc_warehouse from `mini_delta_seed.sql`
(translation of benchmarks/ldbc_snb/data/mini_dataset.sql), then executes
the SQL passed in SMOKE_SQL and prints the result rows.

Bind-mount this directory read-only into the container; the warehouse stays
container-local so host/container uid mismatches don't break Delta log
creation.
"""
import os
import sys
from pathlib import Path

from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip

WAREHOUSE = "/tmp/ldbc_warehouse"
SEED_FILE = Path(__file__).parent / "mini_delta_seed.sql"


def build_spark() -> SparkSession:
    builder = (
        SparkSession.builder
        .appName("clickgraph-spark-smoke")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.warehouse.dir", WAREHOUSE)
        .config("spark.sql.ansi.enabled", "false")
    )
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    return spark


def _split_statements(sql_text: str) -> list[str]:
    """Split a `;`-terminated SQL script into individual statements.

    The seed file is hand-written with no semicolons inside string literals,
    so a naive split on `;` is safe here. Comments (`-- ...` lines) and
    blank lines are tolerated.
    """
    statements: list[str] = []
    buf: list[str] = []
    for raw in sql_text.splitlines():
        line = raw.rstrip()
        if not line or line.lstrip().startswith("--"):
            continue
        buf.append(line)
        if line.endswith(";"):
            stmt = "\n".join(buf).rstrip(";").strip()
            if stmt:
                statements.append(stmt)
            buf = []
    if buf:
        tail = "\n".join(buf).strip()
        if tail:
            statements.append(tail)
    return statements


def seed(spark: SparkSession) -> None:
    sql_text = SEED_FILE.read_text()
    for stmt in _split_statements(sql_text):
        spark.sql(stmt)


def main() -> int:
    sql = os.environ.get("SMOKE_SQL")
    if not sql:
        print("SMOKE_SQL env var not set", file=sys.stderr)
        return 2

    spark = build_spark()
    seed(spark)

    print("=" * 70)
    print("SQL:")
    print(sql)
    print("=" * 70)
    print("RESULT:")
    # n is set high so the parity gate sees every row — `show()` defaults to 20,
    # which would silently truncate larger result sets into a false mismatch (or
    # an arbitrary subset under order-insensitive comparison). truncate=False
    # keeps wide string cells intact.
    spark.sql(sql).show(n=100_000, truncate=False)
    spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
