# Multi-Schema Setup Guide

This directory contains scripts for setting up and testing the multi-schema architecture.

## Quick Start

### 1. Setup Databases and Tables

```bash
# Run the setup script to create all databases and tables
./scripts/setup/setup_multi_schema_databases.sh

# Setup lineage test data (for edge constraint tests)
./scripts/setup/setup_lineage_test_data.sh
```

This creates:
- **4 databases**: brahmand, ldbc, travel, security, lineage
- **30+ tables** across 6+ schemas
- **Sample data** for testing

### 2. Start ClickGraph Server

```bash
export GRAPH_CONFIG_PATH="./schemas/test/unified_test_multi_schema.yaml"
cargo run --bin clickgraph
```

### 3. Test All Schemas

```bash
# Run comprehensive test suite
./scripts/test/test_multi_schema_queries.sh

# Run edge constraint tests (includes VLP with relationship filters)
pytest tests/integration/test_edge_constraints.py -v
```

## Available Setup Scripts

### setup_multi_schema_databases.sh
Sets up core test databases with multiple schemas for testing various features.

### setup_lineage_test_data.sh ⭐ NEW (Dec 27, 2025)
Sets up lineage database for testing edge constraints with VLP queries.

**What it creates**:
- `lineage` database
- `data_files` table (4 files with timestamps)
- `file_lineage` table (4 edges, including one that violates timestamp constraint)

**Use case**: Testing VLP with relationship filters + edge constraints

**Example query**:
```cypher
USE data_lineage
MATCH (f:DataFile {file_id: 1})-[r:COPIED_BY*1..3 {operation: 'clean'}]->(d:DataFile)
RETURN f.path, d.path
```

## Schema Overview

### 1. social_benchmark (brahmand database)
- **Nodes**: User, Post
- **Edges**: AUTHORED, FOLLOWS, LIKED
- **Data**: 5 users, 4 posts, 5 follows, 4 likes

**Example Query**:
```cypher
USE social_benchmark
MATCH (u:User)-[:FOLLOWS]->(friend:User)
RETURN u.name, friend.name
```

### 2. test_fixtures (brahmand database)
- **Nodes**: TestUser, TestProduct, TestGroup
- **Edges**: TEST_PURCHASED, TEST_FRIENDS_WITH, MEMBER_OF, RATED
- **Data**: 3 users, 3 products, 2 groups

**Example Query**:
```cypher
USE test_fixtures
MATCH (u:TestUser)-[:TEST_PURCHASED]->(p:TestProduct)
RETURN u.name, p.name, p.price
```

### 3. ldbc_snb (ldbc database)
- **Nodes**: Person, Comment, Forum, Tag
- **Edges**: KNOWS, HAS_CREATOR, REPLY_OF, LIKES, CONTAINER_OF, HAS_MEMBER, HAS_MODERATOR, HAS_TAG
- **Data**: 3 persons, 2 KNOWS relationships

**Example Query**:
```cypher
USE ldbc_snb
MATCH (p1:Person)-[:KNOWS]->(p2:Person)
RETURN p1.firstName, p1.lastName, p2.firstName, p2.lastName
```

### 4. denormalized_flights (travel database)
- **Nodes**: Airport (denormalized from flights table)
- **Edges**: FLIGHT (with flight properties)
- **Data**: 3 airports, 3 flights

**Example Query**:
```cypher
USE denormalized_flights
MATCH (origin:Airport)-[f:FLIGHT]->(dest:Airport)
RETURN origin.code, dest.code, f.flight_number
```

### 5. pattern_comp (brahmand database)
- **Nodes**: PatternCompUser
- **Edges**: PATTERN_COMP_FOLLOWS
- **Data**: 3 users, 2 follows

**Example Query**:
```cypher
USE pattern_comp
MATCH (u1:PatternCompUser)-[:PATTERN_COMP_FOLLOWS]->(u2:PatternCompUser)
RETURN u1.name, u2.name
```

### 6. zeek_logs (security database)
- **Nodes**: IP, Domain
- **Edges**: DNS_REQUESTED, CONNECTED_TO
- **Data**: 3 IPs, 2 DNS queries, 2 connections

**Example Query**:
```cypher
USE zeek_logs
MATCH (ip:IP)-[:DNS_REQUESTED]->(domain:Domain)
RETURN ip.ip, domain.domain
```

## Manual Testing

### Check Loaded Schemas
```bash
curl -s http://localhost:8080/schemas | jq
```

Expected output:
```json
{
  "schemas": [
    {"name": "social_benchmark", "node_count": 4, "relationship_count": 6},
    {"name": "test_fixtures", "node_count": 6, "relationship_count": 8},
    {"name": "ldbc_snb", "node_count": 8, "relationship_count": 16},
    {"name": "denormalized_flights", "node_count": 2, "relationship_count": 2},
    {"name": "pattern_comp", "node_count": 2, "relationship_count": 2},
    {"name": "zeek_logs", "node_count": 4, "relationship_count": 4},
    {"name": "default", "node_count": 4, "relationship_count": 6}
  ]
}
```

### Test Individual Schema
```bash
# social_benchmark
curl -X POST http://localhost:8080/query \
  -H "Content-Type: application/json" \
  -d '{"query":"USE social_benchmark MATCH (u:User) RETURN u.name, u.country"}'

# test_fixtures
curl -X POST http://localhost:8080/query \
  -H "Content-Type: application/json" \
  -d '{"query":"USE test_fixtures MATCH (u:TestUser)-[:MEMBER_OF]->(g:TestGroup) RETURN u.name, g.name"}'

# ldbc_snb
curl -X POST http://localhost:8080/query \
  -H "Content-Type: application/json" \
  -d '{"query":"USE ldbc_snb MATCH (p:Person) RETURN p.firstName, p.lastName"}'
```

## Adding More Data

### Scale Up social_benchmark

Use the unified benchmark script for larger datasets:

```bash
cd benchmarks/social_network/data

# 10K users (medium scale)
python setup_unified.py --scale 10

# 100K users (large scale)
python setup_unified.py --scale 100

# 1M users (xlarge scale)
python setup_unified.py --scale 1000
```

### Add LDBC SNB Data

See `benchmarks/ldbc_snb/scripts/` for full LDBC dataset loading.

## Troubleshooting

### Schema Not Loaded
```bash
# Check if tables exist
docker exec clickhouse clickhouse-client -u test_user --password test_pass \
  -q "SELECT database, table, engine FROM system.tables WHERE database IN ('brahmand', 'ldbc', 'travel', 'security')"
```

### Data Not Found
```bash
# Verify row counts
docker exec clickhouse clickhouse-client -u test_user --password test_pass \
  -q "SELECT 'users_bench' as table, count() FROM brahmand.users_bench"
```

### Server Not Finding Schema
```bash
# Verify environment variable
echo $GRAPH_CONFIG_PATH

# Should be: ./schemas/test/unified_test_multi_schema.yaml

# Check file exists
ls -la $GRAPH_CONFIG_PATH
```

## Next Steps

1. **Run Integration Tests**: `pytest tests/integration/`
2. **Run Benchmarks**: `cd benchmarks/social_network && python queries/suite.py`
3. **Load Large Datasets**: Use scale factor 100+ for stress testing
4. **Add Custom Schemas**: Edit `schemas/test/unified_test_multi_schema.yaml`

## Architecture

```
Multi-Schema System
├── unified_test_multi_schema.yaml (6 schemas)
│   ├── default_schema: social_benchmark
│   └── schemas: [social_benchmark, test_fixtures, ldbc_snb, ...]
│
├── Databases (4)
│   ├── brahmand (social_benchmark, test_fixtures, pattern_comp)
│   ├── ldbc (ldbc_snb)
│   ├── travel (denormalized_flights)
│   └── security (zeek_logs)
│
└── USE Clause
    └── Switches between schemas dynamically
```

## Resources

- **Main Documentation**: [docs/schema-reference.md](../../docs/schema-reference.md)
- **Configuration Guide**: [docs/configuration.md](../../docs/configuration.md)
- **Getting Started**: [docs/getting-started.md](../../docs/getting-started.md)
