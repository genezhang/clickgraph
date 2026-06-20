# DeltaGraph Local Testing Results

## Overview

DeltaGraph (Cypher → Spark SQL translation for Databricks) has been tested against **delta-docker** (`deltaio/delta-docker:4.1.0`), a local Spark/Delta Lake environment.

## Test Setup

```bash
# Build cg with databricks feature
cargo build --release -p clickgraph-tool --features databricks

# Run tests
CG_BIN=/mnt/cargo-sd/cargo/target/release/cg \
  CLICKGRAPH_SPARK_TESTS=1 pytest tests/spark_smoke/ -v
```

## Test Results

### ✅ test_short1_flat_join
**Query:** `MATCH (n:Person {id: 1})-[:IS_LOCATED_IN]->(p:City) RETURN n.firstName AS firstName, p.id AS cityId`

**Generated SQL:**
```sql
SELECT 
      n.firstName AS `firstName`, 
      p.id AS `cityId`
FROM ldbc.Person AS n
INNER JOIN ldbc.Person_isLocatedIn_Place AS t1 ON t1.PersonId = n.id
INNER JOIN ldbc.Place AS p ON p.id = t1.CityId
WHERE (n.id = 1 AND (p.type = 'City'))
```

**Result:** ✅ `Alice, 7` (correct)

---

### ✅ test_knows_vlp_recursive_cte
**Query:** `MATCH (p:Person {id: 1})-[:KNOWS*1..2]-(friend:Person) WHERE friend.id <> p.id RETURN DISTINCT friend.id AS friendId, friend.firstName AS firstName ORDER BY friendId`

**Generated SQL:** Two recursive CTEs (`vlp_p_friend`, `vlp_friend_p`) for undirected traversal

**Result:** ✅ `Bob(2), Carol(3), Dave(4)` (correct - all friends within 2 hops)

---

### ✅ test_collect_and_count_aggregation
**Query:** `MATCH (p:Person {id: 1})-[:KNOWS]->(friend:Person) RETURN p.firstName AS anchor, count(friend) AS friendCount, collect(friend.firstName) AS friends`

**Generated SQL:**
```sql
SELECT 
      p.firstName AS `anchor`, 
      count(friend.id) AS `friendCount`, 
      collect_list(friend.firstName) AS `friends`
FROM ldbc.Person AS p
INNER JOIN ldbc.Person_knows_Person AS t1 ON t1.Person1Id = p.id
INNER JOIN ldbc.Person AS friend ON friend.id = t1.Person2Id
WHERE p.id = 1
GROUP BY p.firstName
```

**Key Translation:** `collect()` → `collect_list()` ✅

**Result:** ✅ `Alice, 2, [Bob, Carol]` (correct)

---

### ✅ test_optional_match_null_safe_filter
**Query:** `MATCH (p:Person) OPTIONAL MATCH (p)-[:STUDY_AT]->(u:University) RETURN p.firstName AS firstName, u.name AS uniName ORDER BY p.id`

**Generated SQL:**
```sql
SELECT 
      p.firstName AS `firstName`, 
      u.name AS `uniName`
FROM ldbc.Person AS p
LEFT JOIN ldbc.Person_studyAt_Organisation AS t1 ON t1.PersonId = p.id
LEFT JOIN ldbc.Organisation AS u ON u.id = t1.UniversityId
WHERE ((u.type = 'University') OR u.id IS NULL)
ORDER BY p.id ASC
```

**Key Features:**
- `OPTIONAL MATCH` → `LEFT JOIN` ✅
- NULL-safe schema filter (`OR u.id IS NULL`) ✅

**Result:** ✅ All 5 persons returned with universities (Alice→MIT, Bob/Carol→TU_Berlin, Dave/Eve→NULL)

---

### ✅ test_string_functions_mapping
**Query:** `MATCH (p:Person) WHERE p.firstName STARTS WITH "A" RETURN toUpper(p.firstName) AS upperName, length(p.firstName) AS nameLen ORDER BY p.id`

**Generated SQL:**
```sql
SELECT 
      upper(p.firstName) AS `upperName`, 
      length(p.firstName) AS `nameLen`
FROM ldbc.Person AS p
WHERE startsWith(p.firstName, 'A')
ORDER BY p.id ASC
```

**Key Translations:**
- `STARTS WITH` → `startsWith()` ✅
- `toUpper()` → `upper()` ✅
- `length()` → `length()` ✅

**Result:** ✅ `ALICE, 5` (correct)

---

## Function Mapping Verification

| Cypher Function | Spark SQL Translation | Status |
|----------------|----------------------|--------|
| `collect()` | `collect_list()` | ✅ |
| `count()` | `count()` | ✅ |
| `length()` | `length()` | ✅ |
| `toUpper()` | `upper()` | ✅ |
| `STARTS WITH` | `startsWith()` | ✅ |

---

## VLP (Variable-Length Path) Support

### Undirected Traversal (`*1..2]-`)
- Two recursive CTEs generated (forward + backward)
- Cycle detection via `array_contains(path_nodes, end_node.id)`
- Result deduplication via `UNION DISTINCT`

### Directed Traversal (`*1..2]->`)
- Single recursive CTE
- Proper hop count tracking

---

## LDBC SNB Result-Set Parity (M3 gate)

`tests/spark_smoke/test_ldbc_parity.py` runs each LDBC query that executes on
Delta over **both** backends on the same mini dataset and diffs the result sets
(order-insensitive, with cross-engine value normalization):

- **ClickHouse**: `cg --dialect clickhouse query` against a live ClickHouse
  loaded with `benchmarks/ldbc_snb/data/mini_dataset.sql` (DB `ldbc`).
- **Delta**: `cg --dialect databricks sql` into the `deltaio/delta-docker`
  container seeded by `mini_delta_seed.sql`.

Both sides start from identical Cypher, so a mismatch is a real translation
divergence, not a data difference.

**Result: 22/22 parity-eligible queries match.**

| Query | CH rows | Delta rows | Parity |
|---|---|---|---|
| bi-1 | 2 | 2 | ✅ (incl. boolean + float column) |
| bi-4, bi-11, complex-13 | 1 | 1 | ✅ |
| bi-9 | 4 | 4 | ✅ |
| bi-2/3/5/7/18, complex-2/4/6/7/8/9, short-1/3/4/5/6/7 | 0 | 0 | ✅ (empty-set agreement) |

Two cross-engine **value-rendering** differences were found and normalized in
the harness (not translation bugs): booleans render `0`/`1` in ClickHouse vs
`true`/`false` in Spark, and floats can differ in the last ULP
(`0.4545454545454546` vs `0.45454545454545453`). The canonicalizer folds
booleans to `0`/`1` and compares floats at 12 significant digits.

Excluded from the parity gate (not DeltaGraph issues):
- `complex-1`, `complex-11` — the **ClickHouse**-dialect translation errors on
  the mini dataset (`Code 47 UNKNOWN_IDENTIFIER`); the Delta side executes
  fine, so there's no CH baseline to diff. Tracked in `CH_SIDE_EXCLUSIONS`.

**Caveat — empty-set parity is weak.** The mini dataset is tiny, so many
bi/complex/short queries filter to zero rows; "0 == 0" confirms both sides
translate and execute without error but doesn't exercise result content. Real
content parity comes from bi-1/4/9/11 and complex-13 (and the smoke suite's
flat-join, collect/aggregation, and VLP cases). Closing this fully needs a
**full-scale LDBC datagen** materialized as both ClickHouse and Delta tables —
a GA-tier follow-up (see `GA_READINESS.md`).

To reproduce, see the bring-up block in `test_ldbc_parity.py`'s module
docstring.

## Known Limitations

1. **No full LDBC SNB benchmark** - Only mini dataset (5 persons) tested;
   result-set parity holds on it (see above) but doesn't exercise scale
2. **No performance measurements** - Local Docker environment not suitable for benchmarks
3. **No OAuth M2M auth** - Testing uses direct SQL execution, not Databricks API

---

## Conclusion

**DeltaGraph is fully functional** with delta-docker for local development and testing:

- ✅ All 5 smoke tests pass
- ✅ Cypher → Spark SQL translation correct
- ✅ Function mapping complete
- ✅ VLP with recursive CTEs working
- ✅ OPTIONAL MATCH with NULL handling correct

**Next Steps:**
1. Run full LDBC SNB benchmark against delta-docker (requires larger dataset)
2. Add more complex queries to test edge cases
3. Consider extending Zeta with Databricks REST API for even better local testing
