# ClickHouse-vs-Spark differential (P-4c S12)

Databricks coverage is checked by running each corpus query's bound-plan
SQL on both engines over the same data: the ClickHouse SQL on ClickHouse,
the Databricks SQL on Spark (Delta), and comparing the rows.

Requirements:
- ClickHouse on `localhost:8123` (user `test_user` / `test_pass`) with the test databases loaded;
- the `deltaio/delta-docker:4.1.0` image;
- a work directory `$CG_SPARK_WORK` that is world-writable (the container's user writes into it).

```bash
export CG_SPARK_WORK=/path/to/work && mkdir -p $CG_SPARK_WORK/q && chmod -R 777 $CG_SPARK_WORK
cp scripts/oracle/spark/worker.py $CG_SPARK_WORK/

# 1. The tables the corpus schemas read, exported from ClickHouse.
python3 scripts/oracle/spark/tables_of.py > $CG_SPARK_WORK/schema_tables.json   # needs PyYAML
python3 scripts/oracle/spark/export_ch.py $CG_SPARK_WORK

# 2. A Spark worker that loads them as Delta tables and runs queued SQL.
docker run -d --name cg-spark -v $CG_SPARK_WORK:/work --entrypoint sleep deltaio/delta-docker:4.1.0 infinity
docker exec -d cg-spark bash -lc 'cd /work && python3 worker.py > /work/worker.log 2>&1'
# ready when $CG_SPARK_WORK/q/READY exists; stop it with `docker exec cg-spark kill $(cat $CG_SPARK_WORK/worker.pid)`

# 3. Each dialect's bound-plan SQL of the corpus, then the differential.
CENSUS_SQL_OUT=$CG_SPARK_WORK cargo test --test integration the_corpus_lowers
python3 scripts/oracle/spark/diff.py $CG_SPARK_WORK      # writes $CG_SPARK_WORK/diff.jsonl
```

Ad hoc queries: write one Cypher query per line, then
`python3 scripts/oracle/spark/mkcorpus.py <schema_map name> queries.txt $CG_SPARK_WORK/adhoc.jsonl`
and run step 3 with `CENSUS_CORPUS=$CG_SPARK_WORK/adhoc.jsonl`.

Verdicts:
- `MATCH`: the same rows as a multiset. A final `LIMIT` in a query without `ORDER BY` checks that Spark's rows are as many of ClickHouse's unlimited rows. `collect()` lists compare as multisets, unless an `ORDER BY` comes before the collect.
- `TIES_LIMIT`: `ORDER BY … LIMIT` gave other rows, all among ClickHouse's unlimited rows and as many: rows tied at the cut. Check by hand.
- `ORDER_DIFF`: the same rows in another order. The sort keys are not known to the script, so check by hand that these are ties.
- Booleans compare as numbers: ClickHouse's `UInt8` booleans and Spark's `BOOLEAN` are the same values.
- `MISMATCH`, `SPARK_ERROR`, `CH_ERROR`, `BOTH_ERROR`.
- `NO_DATA`: a table the query reads is not in ClickHouse.

Data mapping:
- ClickHouse integers load as Spark integers (`UInt8` → SMALLINT, `UInt16` → INT, `UInt32` / `UInt64` / `Int64` → BIGINT; the test data fits).
- `is_*` / `has_*` UInt8 columns load as BOOLEAN, as a Databricks table would hold them.
- Schemas whose properties are ClickHouse expressions (`property_expressions`) fail on Spark by design.
