"""Container-side helper for the Spark smoke tests.

Runs inside `deltaio/delta-docker:latest`. Seeds a tiny LDBC slice as Delta
tables under /tmp/ldbc_warehouse, then executes the SQL passed in SMOKE_SQL
and prints the result rows.

Bind-mount this file read-only into the container; the warehouse stays
container-local so host/container uid mismatches don't break Delta log
creation.
"""
import os
import sys
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip

WAREHOUSE = "/tmp/ldbc_warehouse"


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


def seed(spark: SparkSession) -> None:
    spark.sql("CREATE DATABASE IF NOT EXISTS ldbc")

    spark.sql(f"""
    CREATE OR REPLACE TABLE ldbc.Person (
        id            BIGINT,
        firstName     STRING,
        lastName      STRING,
        gender        STRING,
        birthday      DATE,
        creationDate  TIMESTAMP,
        locationIP    STRING,
        browserUsed   STRING,
        email         ARRAY<STRING>,
        speaks        ARRAY<STRING>
    ) USING DELTA LOCATION '{WAREHOUSE}/Person'
    """)
    # id=18 (Eve) deliberately has no IS_LOCATED_IN edge — OPTIONAL MATCH test relies on this.
    spark.sql("""
    INSERT INTO ldbc.Person VALUES
        (14, 'Alice', 'Anderson', 'female', DATE'1990-01-15', TIMESTAMP'2010-01-01 10:00:00', '10.0.0.1', 'Firefox', ARRAY('alice@example.com'), ARRAY('en')),
        (15, 'Bob',   'Brown',    'male',   DATE'1985-06-20', TIMESTAMP'2010-02-02 11:00:00', '10.0.0.2', 'Chrome',  ARRAY('bob@example.com'),   ARRAY('en')),
        (16, 'Carol', 'Clark',    'female', DATE'1992-09-30', TIMESTAMP'2010-03-03 12:00:00', '10.0.0.3', 'Safari',  ARRAY('carol@example.com'), ARRAY('en')),
        (17, 'Dan',   'Davis',    'male',   DATE'1988-04-04', TIMESTAMP'2010-04-04 13:00:00', '10.0.0.4', 'Firefox', ARRAY('dan@example.com'),   ARRAY('en')),
        (18, 'Eve',   'Evans',    'female', DATE'1995-12-12', TIMESTAMP'2010-05-05 14:00:00', '10.0.0.5', 'Chrome',  ARRAY('eve@example.com'),   ARRAY('en'))
    """)

    spark.sql(f"""
    CREATE OR REPLACE TABLE ldbc.Place (
        id    BIGINT,
        name  STRING,
        url   STRING,
        type  STRING
    ) USING DELTA LOCATION '{WAREHOUSE}/Place'
    """)
    spark.sql("""
    INSERT INTO ldbc.Place VALUES
        (1000, 'Springfield',   'http://places/Springfield',  'City'),
        (2000, 'United_States', 'http://places/USA',          'Country'),
        (3000, 'North_America', 'http://places/NorthAmerica', 'Continent')
    """)

    spark.sql(f"""
    CREATE OR REPLACE TABLE ldbc.Person_isLocatedIn_Place (
        PersonId      BIGINT,
        CityId        BIGINT,
        creationDate  TIMESTAMP
    ) USING DELTA LOCATION '{WAREHOUSE}/Person_isLocatedIn_Place'
    """)
    # Persons 14..17 live in Springfield; Eve (18) deliberately has no edge.
    spark.sql("""
    INSERT INTO ldbc.Person_isLocatedIn_Place VALUES
        (14, 1000, TIMESTAMP'2010-01-01 10:00:00'),
        (15, 1000, TIMESTAMP'2010-02-02 11:00:00'),
        (16, 1000, TIMESTAMP'2010-03-03 12:00:00'),
        (17, 1000, TIMESTAMP'2010-04-04 13:00:00')
    """)

    # Friend graph rooted at id=14:
    #   14 -knows-> 15  (forward 1-hop)
    #   15 -knows-> 16  (reachable in 2 hops from 14)
    #   17 -knows-> 14  (reverse edge — 17 is a friend via the reverse CTE branch)
    spark.sql(f"""
    CREATE OR REPLACE TABLE ldbc.Person_knows_Person (
        Person1Id     BIGINT,
        Person2Id     BIGINT,
        creationDate  TIMESTAMP
    ) USING DELTA LOCATION '{WAREHOUSE}/Person_knows_Person'
    """)
    spark.sql("""
    INSERT INTO ldbc.Person_knows_Person VALUES
        (14, 15, TIMESTAMP'2020-01-01 00:00:00'),
        (15, 16, TIMESTAMP'2020-01-02 00:00:00'),
        (17, 14, TIMESTAMP'2020-01-03 00:00:00')
    """)


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
    spark.sql(sql).show(truncate=False)
    spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
