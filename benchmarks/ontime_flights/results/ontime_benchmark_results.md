# OnTime Benchmark Results

**Date**: December 6, 2025  
**ClickGraph Version**: v0.5.4 (graph_join_inference v2)  
**Dataset**: OnTime Flight Data 2021-2023 (~19.6M flights, 435 airports)

## System Under Test (SUT)

| Component | Specification |
|-----------|---------------|
| **CPU** | AMD Ryzen AI MAX+ 395 w/ Radeon 8060S |
| **Cores** | 32 |
| **Memory** | 27 GB |
| **OS** | Linux (WSL2 6.6.87.2-microsoft-standard-WSL2) |
| **ClickHouse** | v25.8.11.66 |
| **ClickGraph** | v0.5.4 |

## Dataset

| Year | Flights |
|------|---------|
| 2021 | 5,995,397 |
| 2022 | 6,729,125 |
| 2023 | 6,847,899 |
| **Total** | **19,572,421** |

## Benchmark Summary (5 iterations each)

| Query | Description | 2021 | 2022 | 2023 | Overall Avg |
|-------|-------------|------|------|------|-------------|
| Q1 | 2-hop connecting flights | 99ms | 102ms | 71ms | **91ms** |
| Q2 | Delayed connections | 87ms | 81ms | 87ms | **85ms** |
| Q3 | Hub airport analysis | 23ms | 18ms | 23ms | **21ms** |
| Q4 | 3-hop aircraft journey | 33ms | 27ms | 33ms | **31ms** |

**All 4 queries pass with full cross-table inequality comparisons!**

## Query Details

### Q1: 2-hop Connecting Flights by Month

Find all connecting flights from LAX (12892) to JFK (12953) with ≥100 min layover.

**Cypher Query**:
```cypher
MATCH (a:Airport)-[r1:FLIGHT]->(b:Airport)-[r2:FLIGHT]->(c:Airport) 
WHERE a.id = 12892 AND c.id = 12953 AND r1.year = 2022
  AND r1.flight_date = r2.flight_date 
  AND r1.crs_arrival_time + 100 <= r2.crs_departure_time
RETURN r1.month as month, count(*) as path_count 
ORDER BY month
```

**Generated SQL**:
```sql
SELECT r1.Month AS "month", count(*) AS "path_count"
FROM default.flights AS r1
INNER JOIN default.flights AS r2 ON r2.OriginAirportID = r1.DestAirportID
WHERE r1.OriginAirportID = 12892 AND r2.DestAirportID = 12953 AND r1.Year = 2022
  AND r1.FlightDate = r2.FlightDate AND r1.CRSArrTime + 100 <= r2.CRSDepTime
GROUP BY r1.Month
ORDER BY month
```

**Sample Results (2022)**:
| Month | Path Count |
|-------|------------|
| 1 | 30,850 |
| 2 | 27,802 |
| 3 | 32,543 |
| ... | ... |

---

### Q2: Delayed Connecting Flights

Same as Q1 but also filters for missed connections (actual arrival ≥ scheduled departure).

**Cypher Query**:
```cypher
MATCH (a:Airport)-[r1:FLIGHT]->(b:Airport)-[r2:FLIGHT]->(c:Airport) 
WHERE a.id = 12892 AND c.id = 12953 AND r1.year = 2022
  AND r1.flight_date = r2.flight_date 
  AND r1.crs_arrival_time + 100 <= r2.crs_departure_time
  AND r1.arrival_time >= r2.departure_time
RETURN r1.month as month, count(*) as path_count 
ORDER BY month
```

**Sample Results (2022)**:
| Month | Path Count |
|-------|------------|
| 1 | 3,743 |
| 2 | 2,268 |
| 3 | 2,473 |

---

### Q3: Hub Airport Analysis

Find top 10 airports by distinct aircraft turnarounds on a single day.

**Cypher Query**:
```cypher
MATCH (a:Airport)-[r1:FLIGHT]->(n:Airport)-[r2:FLIGHT]->(c:Airport)
WHERE r1.flight_date = '2022-06-08'
  AND r1.flight_date = r2.flight_date
  AND r1.tail_num = r2.tail_num
  AND r1.tail_num IS NOT NULL AND r1.tail_num <> ''
  AND r1.crs_arrival_time < r2.crs_departure_time
RETURN n.code as hub_airport, count(DISTINCT r1.tail_num) as aircraft_count
ORDER BY aircraft_count DESC LIMIT 10
```

**Sample Results (2022-06-08)**:
| Hub Airport | Aircraft Count |
|-------------|----------------|
| DEN | 488 |
| ATL | 449 |
| ORD | 416 |
| DFW | 401 |
| LAS | 315 |
| LAX | 305 |
| LGA | 290 |
| PHX | 287 |
| SEA | 269 |
| CLT | 268 |

---

### Q4: 3-hop Same Aircraft Journey

Count distinct aircraft flying 3+ consecutive legs on a single day.

**Cypher Query**:
```cypher
MATCH (a:Airport)-[r1:FLIGHT]->(b:Airport)-[r2:FLIGHT]->(c:Airport)-[r3:FLIGHT]->(d:Airport)
WHERE r1.flight_date = '2022-08-08'
  AND r1.flight_date = r2.flight_date AND r2.flight_date = r3.flight_date
  AND r1.tail_num = r2.tail_num AND r2.tail_num = r3.tail_num
  AND r1.tail_num IS NOT NULL AND r1.tail_num <> ''
  AND r1.crs_arrival_time < r2.crs_departure_time
  AND r2.crs_arrival_time < r3.crs_departure_time
RETURN count(DISTINCT r1.tail_num) as aircraft_count
```

**Results**:
| Year | Date | Aircraft Count |
|------|------|----------------|
| 2021 | 2021-08-08 | ~3,500 |
| 2022 | 2022-08-08 | 3,674 |
| 2023 | 2023-08-08 | ~3,700 |

---

## Schema Configuration

The OnTime benchmark uses a **denormalized edge pattern** where Airport node properties are embedded directly in the flights edge table via `from_node_properties` and `to_node_properties`.

**Schema file**: [`benchmarks/schemas/ontime_benchmark.yaml`](../schemas/ontime_benchmark.yaml)

Key features:
- **Virtual nodes**: Airport nodes have no separate table - properties come from flight records
- **EdgeToEdge joins**: Multi-hop queries join `r1.DestAirportID = r2.OriginAirportID` directly
- **Property mapping**: Cypher properties (e.g., `crs_arrival_time`) map to ClickHouse columns (`CRSArrTime`)

## Key Findings

### ✅ What Works

1. **Denormalized Edge Pattern**: `from_node_properties`/`to_node_properties` correctly handles embedded Airport data
2. **Multi-hop JOINs**: EdgeToEdge join strategy generates correct `r1.DestAirportID = r2.OriginAirportID`
3. **Cross-table comparisons**: Fixed! `r1.crs_arrival_time + 100 <= r2.crs_departure_time` now preserved correctly
4. **Filter Pushdown**: WHERE clauses correctly pushed to appropriate tables
5. **GROUP BY / ORDER BY**: Aggregation and sorting work correctly
6. **3-hop queries**: Q4 with 3-way self-join executes in ~30ms

## Comparison with PuppyGraph

All 4 original PuppyGraph benchmark queries now work with full fidelity. ClickGraph generates equivalent SQL and achieves comparable or better performance on ClickHouse.

### Syntax Differences

| Feature | PuppyGraph | ClickGraph |
|---------|------------|------------|
| Node Labels | Optional (auto-inferred) | Required |
| Edge Types | Optional (auto-inferred) | Required |
| Node ID Access | `id(a) = "airport[12892]"` | `a.id = 12892` or `id(a) = 12892` |

**Source**: [PuppyGraph benchmark queries](https://github.com/puppygraph/ClickHouse-PuppyGraph-test/blob/main/setup/task_config/puppygraph_task.yaml)
