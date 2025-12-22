# LDBC Query SQL Generation Audit Report

**Generated**: 2025-12-21T09:15:27.827850

**Database**: ldbc

**ClickGraph**: http://localhost:8080

## Summary

- Total Queries Processed: 46
- ✓ Passed: 42
- ✗ Failed: 4
- Success Rate: 91%

## Results by Query

| Query | Status | Message | SQL File |
|-------|--------|---------|----------|
| BI-1a | ✓ | Success (223 chars) | BI-1a.sql |
| BI-1b | ✓ | Success (71 chars) | BI-1b.sql |
| BI-2a | ✓ | Success (258 chars) | BI-2a.sql |
| BI-2b | ✓ | Success (282 chars) | BI-2b.sql |
| BI-3 | ✓ | Success (1500 chars) | BI-3.sql |
| BI-4a | ✓ | Success (336 chars) | BI-4a.sql |
| BI-4b | ✓ | Success (410 chars) | BI-4b.sql |
| BI-5 | ✓ | Success (636 chars) | BI-5.sql |
| BI-5 | ✓ | Success (1608 chars) | BI-5.sql |
| BI-6 | ✓ | Success (773 chars) | BI-6.sql |
| BI-7 | ✓ | Success (800 chars) | BI-7.sql |
| BI-8a | ✓ | Success (289 chars) | BI-8a.sql |
| BI-8b | ✓ | Success (566 chars) | BI-8b.sql |
| BI-9 | ✓ | Success (669 chars) | BI-9.sql |
| BI-10a | ✓ | Success (320 chars) | BI-10a.sql |
| BI-10b | ✓ | Success (1997 chars) | BI-10b.sql |
| BI-11 | ✓ | Success (405 chars) | BI-11.sql |
| BI-12 | ✓ | Success (910 chars) | BI-12.sql |
| BI-13 | ✓ | Success (1442 chars) | BI-13.sql |
| BI-14 | ✓ | Success (2264 chars) | BI-14.sql |
| BI-16 | ✓ | Success (1075 chars) | BI-16.sql |
| BI-17 | ✓ | Success (1271 chars) | BI-17.sql |
| BI-18 | ✓ | Success (3493 chars) | BI-18.sql |
| AGG-1 | ✓ | Success (402 chars) | AGG-1.sql |
| AGG-2 | ✓ | Success (455 chars) | AGG-2.sql |
| AGG-3 | ✓ | Success (542 chars) | AGG-3.sql |
| AGG-4 | ✓ | Success (284 chars) | AGG-4.sql |
| AGG-5 | ✓ | Success (500 chars) | AGG-5.sql |
| COMPLEX-1 | ✓ | Success (1798 chars) | COMPLEX-1.sql |
| COMPLEX-2 | ✓ | Success (1744 chars) | COMPLEX-2.sql |
| COMPLEX-3 | ✓ | Success (1678 chars) | COMPLEX-3.sql |
| COMPLEX-4 | ✓ | Success (1614 chars) | COMPLEX-4.sql |
| COMPLEX-5 | ✓ | Success (787 chars) | COMPLEX-5.sql |
| bi-4-workaround | ✗ | Planning error: LogicalPlanError: Query planning error: Pars | None |
| bi-8-workaround | ✗ | Planning error: LogicalPlanError: Query planning error: Pars | None |
| interactive-complex-1 | ✓ | Success (5282 chars) | interactive-complex-1.sql |
| interactive-complex-10-workaround | ✗ | Planning error: LogicalPlanError: Query planning error: Pars | None |
| interactive-complex-1_cleaned | ✓ | Success (5282 chars) | interactive-complex-1_cleaned.sql |
| interactive-complex-2 | ✓ | Success (1406 chars) | interactive-complex-2.sql |
| interactive-complex-3 | ✓ | Success (5557 chars) | interactive-complex-3.sql |
| interactive-complex-9 | ✓ | Success (5079 chars) | interactive-complex-9.sql |
| interactive-short-1 | ✓ | Success (470 chars) | interactive-short-1.sql |
| interactive-short-2 | ✓ | Success (344 chars) | interactive-short-2.sql |
| interactive-short-3 | ✓ | Success (753 chars) | interactive-short-3.sql |
| interactive-short-5 | ✓ | Success (274 chars) | interactive-short-5.sql |
| interactive-short-7 | ✗ | Planning error: LogicalPlanError: Query planning error: Pars | None |

## Failed Queries

### bi-4-workaround

**File**: queries/adapted/bi-4-workaround.cypher

**Error**: Planning error: LogicalPlanError: Query planning error: Parser returned empty query AST. This indicates unsupported syntax or parser failure. Common causes: 1) Multi-line SQL-style comments (use /* */

### bi-8-workaround

**File**: queries/adapted/bi-8-workaround.cypher

**Error**: Planning error: LogicalPlanError: Query planning error: Parser returned empty query AST. This indicates unsupported syntax or parser failure. Common causes: 1) Multi-line SQL-style comments (use /* */

### interactive-complex-10-workaround

**File**: queries/adapted/interactive-complex-10-workaround.cypher

**Error**: Planning error: LogicalPlanError: Query planning error: Parser returned empty query AST. This indicates unsupported syntax or parser failure. Common causes: 1) Multi-line SQL-style comments (use /* */

### interactive-short-7

**File**: queries/adapted/interactive-short-7.cypher

**Error**: Planning error: LogicalPlanError: Query planning error: Parser returned empty query AST. This indicates unsupported syntax or parser failure. Common causes: 1) Multi-line SQL-style comments (use /* */

## Next Steps

1. Review generated SQL files in `results/generated_sql/`
2. Verify SQL correctness against LDBC spec
3. Check for optimization opportunities
4. Test SQL execution against ClickHouse
5. Compare results with Neo4j/expected outputs
