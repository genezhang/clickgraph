# DeltaGraph Beat-2 demo — TPC-DS co-purchase over Databricks

`deltagraph-tpcds-copurchase.ipynb` runs a "customers who bought this also bought"
co-purchase traversal as **one Cypher query**, translated to Spark SQL and executed
in place on Databricks' own 1 TB TPC-DS benchmark (`samples.tpcds_sf1000`). No graph
database, nothing copied.

The notebook is a plain `requests` + `pandas` client against the DeltaGraph HTTP
endpoint — no graph-notebook extension required. It ships with baked outputs from a
verified live run (warm path: scale count 0.8 s, traversal **1.18 s** over ~2.75 B
purchase edges).

## Run it

1. **Databricks credentials** (env only — never on the CLI):
   ```bash
   export DATABRICKS_HOST=...            # workspace host, no scheme
   export DATABRICKS_WAREHOUSE_ID=...    # target SQL Warehouse
   export DATABRICKS_TOKEN=...           # PAT
   ```

2. **Start DeltaGraph** with the TPC-DS schema (HTTP on 7477 so it doesn't collide
   with the social-demo server on 7476):
   ```bash
   GRAPH_CONFIG_PATH=demos/graph-notebook/tpcds_copurchase_databricks.yaml \
     ./target/release/deltagraph --http-port 7477 --bolt-port 7689
   ```

3. **Open the notebook** and point it at that endpoint (default already matches):
   ```bash
   export DELTAGRAPH_HTTP=http://localhost:7477/query
   jupyter lab demos/graph-notebook/deltagraph-tpcds-copurchase.ipynb
   ```

## ⚠ Pre-warm before recording

The free-tier SQL Warehouse **auto-suspends when idle**, so the first query pays a
~15–30 s cold start. Run the **Setup** cell once ~30 s before you hit record; it
absorbs the cold start off camera, and the demo cells then run on the warm ~1 s path.
The "~1 second, over a terabyte" caption is honest *only* on the warm path.

## Notes

- `store_sales` = ~2.88 B rows raw; the graph reports **~2.75 B purchase edges** —
  the difference is TPC-DS rows with a null customer or item key, which can't form an
  edge. Both numbers are honest; the notebook shows the edge count.
- TPC-DS product names are synthetic ("priought", "oughtesen stationn stought") — real
  benchmark data, generator artifacts and all. The ranking and shopper counts are the
  actual output. (Grouping by `i_category` is *not* a good alternative headline: TPC-DS
  distributes purchases uniformly, so every category returns an identical count.)
- The 2-hop bounded traversal lowers to a plain FK-edge **self-join**, not a recursive
  CTE — don't mislabel it as one. (Unbounded/variable-length paths *do* generate
  recursive CTEs.)
