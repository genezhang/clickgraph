# Schemas Directory

**Last Updated**: December 20, 2025

## Directory Structure

```
schemas/
├── examples/           # Example schemas for documentation
│   ├── ecommerce_simple.yaml
│   ├── ecommerce_graph_demo.yaml
│   ├── ecommerce_benchmark.yaml
│   ├── filesystem.yaml
│   ├── filesystem_single.yaml
│   ├── social_network.yaml
│   ├── social_polymorphic.yaml
│   ├── multi_graph_benchmark.yaml
│   ├── multi_tenant_simple.yaml
│   ├── multi_tenant_encrypted.yaml
│   ├── zeek_conn_log.yaml
│   ├── zeek_dns_log.yaml
│   ├── zeek_merged.yaml
│   ├── ontime_denormalized.yaml
│   ├── ontime_denormalized_mismatched.yaml
│   ├── group_membership.yaml
│   ├── orders_customers_fk.yaml
│   ├── composite_node_id_test.yaml
│   ├── auto_discovery_demo.yaml
│   └── security_graph.yaml
│
└── test/              # Test-specific schemas
    ├── composite_node_ids.yaml
    ├── expression_test.yaml
    ├── expression_integration_test.yaml
    ├── filter_test.yaml
    ├── multi_tenant.yaml
    ├── multi_rel_test.yaml
    ├── test_multi_rel_schema.yaml
    ├── test_multiple_rels.yaml
    ├── test_friendships.yaml
    ├── test_integration_correct.yaml
    ├── test_integration_schema.yaml
    ├── denormalized_flights.yaml
    └── mixed_denorm_test.yaml
```

## Primary Schemas

**Production benchmarks** are in `benchmarks/*/schemas/`:
- `benchmarks/social_network/schemas/social_benchmark.yaml` - **PRIMARY DEVELOPMENT SCHEMA**
- `benchmarks/ldbc_snb/schemas/ldbc_snb_complete.yaml` - LDBC SNB benchmark
- `benchmarks/ontime_flights/schemas/ontime_benchmark.yaml` - OnTime flights

## Schema Categories

### Examples (for documentation)
- **E-commerce**: `ecommerce_*.yaml` - Product catalog and orders
- **Social networks**: `social_*.yaml` - Users, posts, follows
- **File systems**: `filesystem*.yaml` - Directories and files
- **Network security**: `zeek_*.yaml`, `security_graph.yaml` - Network logs
- **Flights**: `ontime_*.yaml` - Flight data
- **Multi-tenancy**: `multi_tenant_*.yaml` - Tenant isolation patterns
- **Groups**: `group_membership.yaml` - Hierarchical groups

### Test Schemas
- Used by integration tests
- Located in `schemas/test/`
- Suite-specific schemas in `tests/integration/suites/*/schema.yaml`

## Schema Field Standard

All schemas use the **`edges:`** field (not `relationships:`):

```yaml
graph_schema:
  nodes:
    - label: User
      database: mydb
      table: users
      node_id: user_id
      property_mappings:
        name: full_name

  edges:  # ✅ Use "edges:" not "relationships:"
    - type: FOLLOWS
      database: mydb
      table: follows
      from_id: follower_id
      to_id: followed_id
      from_node: User
      to_node: User
```

## Quick Start

```bash
# Use primary benchmark schema
export GRAPH_CONFIG_PATH="./benchmarks/social_network/schemas/social_benchmark.yaml"

# Or use an example
export GRAPH_CONFIG_PATH="./schemas/examples/ecommerce_simple.yaml"

# Start server
./target/release/clickgraph
```

## Documentation

See `docs/wiki/Schema-Basics.md` and `docs/wiki/Schema-Configuration-Advanced.md` for detailed schema documentation.

## Cleanup History

- **Dec 20, 2025**: Consolidated schema directories
  - Merged `schemas/demo/` → `schemas/examples/`
  - Merged `schemas/tests/` → `schemas/test/`
  - Moved `examples/*.yaml` → `schemas/examples/` or `archive/schemas/`
  - Updated all schemas to use `edges:` field (not `relationships:`)
