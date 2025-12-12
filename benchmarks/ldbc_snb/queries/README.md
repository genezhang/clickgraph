# LDBC SNB Benchmark Queries

This directory contains LDBC Social Network Benchmark (SNB) queries organized into official and adapted categories.

## Directory Structure

```
queries/
├── README.md              # This file
├── official/              # Official LDBC benchmark queries (unmodified)
│   ├── interactive/       # SNB Interactive workload (IS1-IS7, IC1-IC14)
│   │   ├── short-1.cypher through short-7.cypher
│   │   └── complex-1.cypher through complex-14.cypher
│   └── bi/                # SNB Business Intelligence workload (BI1-BI20)
│       └── bi-1.cypher through bi-20.cypher
└── adapted/               # Simplified queries for ClickGraph testing
    ├── bi-queries-adapted.cypher
    ├── ldbc-interactive-adapted.cypher
    └── interactive-*.cypher
```

## Official Queries

### Source Repositories
- **Interactive Queries**: https://github.com/ldbc/ldbc_snb_interactive_v2_impls/tree/main/cypher/queries
- **BI Queries**: https://github.com/ldbc/ldbc_snb_bi/tree/main/neo4j/queries

### Query Categories

#### Interactive Short (IS1-IS7)
Fast lookup queries designed for low-latency responses:
- IS1: Person profile lookup
- IS2: Recent messages of a person
- IS3: Friends of a person
- IS4: Content of a message
- IS5: Creator of a message
- IS6: Forum of a message
- IS7: Recent replies to a message

#### Interactive Complex (IC1-IC14)
More sophisticated analytical queries:
- IC1: Transitive friends with certain name
- IC2: Recent messages from friends
- IC3: Friends within certain countries in period
- IC4: New topics of interest
- IC5: New group membership
- IC6: Tag co-occurrence
- IC7: Recent likes
- IC8: Recent replies
- IC9: Recent messages from friends (extended)
- IC10: Friends recommendation by birthday
- IC11: Job referral
- IC12: Expert search
- IC13: Single shortest path
- IC14: Trusted connection paths

#### Business Intelligence (BI1-BI20)
Complex analytical queries for OLAP workloads:
- BI1: Posting summary
- BI2: Tag evolution  
- BI3: Popular topics in a country
- BI4: Top message creators
- BI5: Active posters
- BI6: Most authoritative users
- BI7: Message count by author
- BI8: Related topics
- BI9: Forum with related tags
- BI10: Experts in social circle
- BI11: Unrelated replies
- BI12: Trending posts
- BI13: Popular tags per month
- BI14: Top thread initiators
- BI15: Social normals vs celebrities
- BI16: Experts in multiple forums  
- BI17: Information propagation analysis
- BI18: How many persons have a given number of posts
- BI19: Stranger interaction (graph algorithm)
- BI20: High-level topics (graph algorithm)

## Adapted Queries

The `adapted/` directory contains **simplified versions** of LDBC queries:
- Removed or simplified parameters for easier testing
- Reduced complexity to focus on specific Cypher features
- May not produce identical results to official queries
- Useful for development and feature validation

**⚠️ WARNING**: Adapted queries are NOT suitable for official benchmark comparisons.

## Usage

### For Benchmark Testing (use official)
```bash
# Run official IC1 query
cat official/interactive/complex-1.cypher | curl -X POST http://localhost:8080/query \
  -H "Content-Type: application/json" \
  -d @- # (with parameter substitution)
```

### For Development Testing (use adapted)
```bash
# Test specific feature with adapted query
cat adapted/bi-queries-adapted.cypher | head -30
```

## Query Support Status

See `/benchmarks/ldbc_snb/README.md` for detailed support status and which queries ClickGraph can execute.

## Notes

1. **Parameters**: Official queries use Cypher parameters (`$personId`, `$datetime`, etc.). ClickGraph supports parameter substitution via the query API.

2. **Graph Algorithms**: BI-19 and BI-20 require graph algorithm support (e.g., shortest path with specific constraints). Status varies.

3. **Write Queries**: LDBC also includes update/delete queries which are out of scope for ClickGraph (read-only engine).
