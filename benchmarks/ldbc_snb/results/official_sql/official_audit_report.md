# LDBC Official Queries - SQL Generation Audit

**Generated**: 2025-12-21T09:22:44.197657

**Database**: ldbc

**ClickGraph**: http://localhost:8080

**Note**: This audit includes ONLY official LDBC SNB queries.

## Summary

- Total Official Queries: 41
- ✓ SQL Generation Success: 25
- ✗ SQL Generation Failed: 16
- Success Rate: 60%

## Results by Category

### BI: 9/20 (45%)

### IC: 16/21 (76%)

## Detailed Results

| Query | Category | Status | Message |
|-------|----------|--------|----------|
| BI-bi-1 | BI | ✓ | OK (324 chars) |
| BI-bi-10 | BI | ✗ | Invalid SQL |
| BI-bi-11 | BI | ✓ | OK (10010 chars) |
| BI-bi-12 | BI | ✓ | OK (2146 chars) |
| BI-bi-13 | BI | ✗ | Planning: AnalyzerError: Property 'creationDate' not found o |
| BI-bi-14 | BI | ✗ | Planning: LogicalPlanError: WITH clause validation error: Ex |
| BI-bi-15 | BI | ✗ | Invalid SQL |
| BI-bi-16 | BI | ✗ | Planning: AnalyzerError: Property 'letter' not found on node |
| BI-bi-17 | BI | ✗ | Invalid SQL |
| BI-bi-18 | BI | ✓ | OK (3573 chars) |
| BI-bi-19 | BI | ✗ | Invalid SQL |
| BI-bi-2 | BI | ✗ | Render: RENDER_ERROR: Invalid render plan: Cannot find ID co |
| BI-bi-20 | BI | ✗ | Invalid SQL |
| BI-bi-3 | BI | ✓ | OK (2451 chars) |
| BI-bi-4 | BI | ✓ | OK (1670 chars) |
| BI-bi-5 | BI | ✗ | Render: RENDER_ERROR: Invalid render plan: Cannot find ID co |
| BI-bi-6 | BI | ✓ | OK (1223 chars) |
| BI-bi-7 | BI | ✓ | OK (792 chars) |
| BI-bi-8 | BI | ✗ | Invalid SQL |
| BI-bi-9 | BI | ✓ | OK (2017 chars) |
| IC-complex-1 | IC | ✗ | Render: RENDER_ERROR: Invalid render plan: Cannot find ID co |
| IC-complex-10 | IC | ✗ | Render: RENDER_ERROR: Invalid render plan: Cannot find ID co |
| IC-complex-11 | IC | ✓ | OK (5764 chars) |
| IC-complex-12 | IC | ✓ | OK (929 chars) |
| IC-complex-13 | IC | ✓ | OK (3331 chars) |
| IC-complex-14 | IC | ✓ | OK (5969 chars) |
| IC-complex-2 | IC | ✓ | OK (1611 chars) |
| IC-complex-3 | IC | ✓ | OK (2107 chars) |
| IC-complex-4 | IC | ✗ | Render: RENDER_ERROR: Invalid render plan: Cannot find ID co |
| IC-complex-5 | IC | ✓ | OK (6587 chars) |
| IC-complex-6 | IC | ✓ | OK (2976 chars) |
| IC-complex-7 | IC | ✗ | Planning: AnalyzerError: Property 'likeTime' not found on no |
| IC-complex-8 | IC | ✓ | OK (962 chars) |
| IC-complex-9 | IC | ✗ | Planning: AnalyzerError: Property 'id' not found on node 'fr |
| IC-short-1 | IC | ✓ | OK (467 chars) |
| IC-short-2 | IC | ✓ | OK (2520 chars) |
| IC-short-3 | IC | ✓ | OK (749 chars) |
| IC-short-4 | IC | ✓ | OK (162 chars) |
| IC-short-5 | IC | ✓ | OK (275 chars) |
| IC-short-6 | IC | ✓ | OK (1677 chars) |
| IC-short-7 | IC | ✓ | OK (2395 chars) |

## Failed Queries

### BI-bi-10

**Category**: BI

**Error**: Invalid SQL

### BI-bi-13

**Category**: BI

**Error**: Planning: AnalyzerError: Property 'creationDate' not found on node 'zombie'

### BI-bi-14

**Category**: BI

**Error**: Planning: LogicalPlanError: WITH clause validation error: Expression without alias: `AggregateFnCall(AggregateFnCall { name: "collect", args: [MapLiteral([("score", TableAlias(TableAlias("score"

### BI-bi-15

**Category**: BI

**Error**: Invalid SQL

### BI-bi-16

**Category**: BI

**Error**: Planning: AnalyzerError: Property 'letter' not found on node 'param'

### BI-bi-17

**Category**: BI

**Error**: Invalid SQL

### BI-bi-19

**Category**: BI

**Error**: Invalid SQL

### BI-bi-2

**Category**: BI

**Error**: Render: RENDER_ERROR: Invalid render plan: Cannot find ID column for alias 'countWindow1' needed for GROUP BY aggregation. This alias may come from a CTE that hasn't been properly registered.

### BI-bi-20

**Category**: BI

**Error**: Invalid SQL

### BI-bi-5

**Category**: BI

**Error**: Render: RENDER_ERROR: Invalid render plan: Cannot find ID column for alias 'likeCount' needed for GROUP BY aggregation. This alias may come from a CTE that hasn't been properly registered.

### BI-bi-8

**Category**: BI

**Error**: Invalid SQL

### IC-complex-1

**Category**: IC

**Error**: Render: RENDER_ERROR: Invalid render plan: Cannot find ID column for alias 'distance' needed for GROUP BY aggregation. This alias may come from a CTE that hasn't been properly registered.

### IC-complex-10

**Category**: IC

**Error**: Render: RENDER_ERROR: Invalid render plan: Cannot find ID column for alias 'friend' needed for GROUP BY aggregation. This alias may come from a CTE that hasn't been properly registered.

### IC-complex-4

**Category**: IC

**Error**: Render: RENDER_ERROR: Invalid render plan: Cannot find ID column for alias 'tag' needed for GROUP BY aggregation. This alias may come from a CTE that hasn't been properly registered.

### IC-complex-7

**Category**: IC

**Error**: Planning: AnalyzerError: Property 'likeTime' not found on node 'latestLike'

### IC-complex-9

**Category**: IC

**Error**: Planning: AnalyzerError: Property 'id' not found on node 'friend'

## Benchmarking Recommendation

For official LDBC SNB benchmarking, use only queries marked with ✓.

These queries match the official LDBC specification and can be compared
with results from other graph databases (Neo4j, TigerGraph, etc.)
