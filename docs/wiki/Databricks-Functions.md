# Databricks / Spark Function Pass-Through

When ClickGraph runs against a **Databricks SQL Warehouse** (the DeltaGraph
backend, Spark SQL dialect), the `dbx.` prefix gives Cypher queries direct
access to **any native Databricks / Spark SQL function** that has no Cypher
built-in.

> This is the Databricks counterpart to ClickHouse's
> [`ch.`/`chagg.` pass-through](ClickHouse-Functions.md). The two are
> **backend-specific and mutually exclusive**: `dbx.` works only on the
> Databricks backend, `ch.`/`chagg.` only on ClickHouse. Using the wrong
> prefix for the active backend is rejected at translation time with a
> message pointing at the right one — it never silently produces wrong SQL.

## Quick reference

| Prefix | Use case | GROUP BY |
|--------|----------|----------|
| `dbx.` | **Any** scalar *or* aggregate Spark/Databricks function | Auto for aggregates |

```cypher
-- Scalar function
MATCH (u:User) RETURN dbx.get_json_object(u.metadata, '$.tier') AS tier

-- Aggregate (GROUP BY added automatically)
MATCH (u:User) RETURN u.country, dbx.percentile_approx(u.score, 0.95) AS p95

-- Aggregate that collects into an array
MATCH (u:User) RETURN u.country, dbx.collect_list(u.id) AS ids
```

The bare function name (prefix stripped) is emitted directly into the
generated Spark SQL: `dbx.percentile_approx(u.score, 0.95)` →
`percentile_approx(score, 0.95)`. Arguments still go through ClickGraph's
normal property mapping and parameter substitution.

## One prefix, not two — why there's no `dbxagg.`

ClickHouse has **two** prefixes (`ch.` for scalars / known aggregates,
`chagg.` to force aggregate treatment) for a historical reason: the
pass-through shipped before an aggregate registry existed, so users had to
declare a function's type themselves.

Databricks ships with a registry of Spark's built-in aggregate functions
from day one, so ClickGraph **infers** scalar-vs-aggregate itself — a single
`dbx.` prefix is enough. Spark's aggregate surface is bounded and
enumerable (unlike ClickHouse's open-ended combinator space), so the
registry can be effectively complete.

- `dbx.<aggregate>` → recognized as an aggregate, GROUP BY added.
- `dbx.<anything else>` → treated as a scalar.

### If an aggregate isn't recognized

If you call a Spark aggregate (or a user-defined UDAF) that isn't yet in
ClickGraph's registry, it will be treated as a *scalar* and GROUP BY won't
be generated — producing a Spark error. The fix is to **add the function to
the registry** (`SPARK_AGGREGATE_FUNCTIONS` in
`src/sql_generator/passthrough/databricks.rs`), not to learn a second
prefix. Please open an issue or PR if you hit a missing one.

## Recognized aggregate functions

The registry tracks Spark / Databricks SQL built-in aggregates, including:

| Category | Functions |
|----------|-----------|
| **Basic** | `count`, `count_if`, `sum`, `avg`, `mean`, `min`, `max`, `min_by`, `max_by`, `first`, `last`, `any_value`, `mode`, `product` |
| **Collection** | `collect_list`, `collect_set`, `array_agg` |
| **Cardinality / sketch** | `approx_count_distinct`, `count_min_sketch`, `hll_sketch_agg`, `hll_union_agg` |
| **Percentiles** | `median`, `percentile`, `percentile_approx`, `approx_percentile`, `histogram_numeric` |
| **Statistics** | `stddev`, `stddev_samp`, `stddev_pop`, `variance`, `var_samp`, `var_pop`, `skewness`, `kurtosis`, `corr`, `covar_samp`, `covar_pop` |
| **Regression** | `regr_avgx`, `regr_avgy`, `regr_count`, `regr_intercept`, `regr_r2`, `regr_slope`, `regr_sxx`, `regr_sxy`, `regr_syy` |
| **Boolean / bitwise** | `any`, `some`, `every`, `bool_and`, `bool_or`, `bit_and`, `bit_or`, `bit_xor`, `bitmap_construct_agg`, `bitmap_or_agg` |
| **Grouping** | `grouping`, `grouping_id` |
| **`try_*` variants** | `try_avg`, `try_sum` |

Everything else under `dbx.` is treated as a scalar function — e.g.
`dbx.get_json_object`, `dbx.element_at`, `dbx.array_contains`, `dbx.upper`,
`dbx.regexp_extract`, `dbx.from_unixtime`, …

See the
[Databricks SQL built-in functions reference](https://docs.databricks.com/aws/en/sql/language-manual/sql-ref-functions-builtin)
for the full catalog.

## Notes

- **Argument shapes are passed through verbatim.** ClickGraph does not
  rewrite a native function's argument conventions — you write them as Spark
  expects (e.g. JSONPath `'$.field'` for `dbx.get_json_object`).
- **Cypher built-ins are still translated** the normal way; only use `dbx.`
  for functions ClickGraph doesn't already map.

## See also

- [ClickHouse Function Pass-Through](ClickHouse-Functions.md) — the `ch.` / `chagg.` equivalent
- [Databricks Deployment (DeltaGraph)](Databricks-Deployment.md)
- [Cypher Functions](Cypher-Functions.md)
