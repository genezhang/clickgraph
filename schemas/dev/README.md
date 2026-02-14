# Development Schemas

This directory contains modified schemas used for testing and development that don't belong in the benchmarks.

## Browser Testing Schemas (`db_xxx` Convention)

Each schema variation has its own isolated database for reproducible browser testing.
Setup all databases at once: `bash scripts/setup/setup_all_db_xxx.sh`

| Schema | Database | Schema File | Setup Script | Pattern |
|--------|----------|-------------|-------------|---------|
| **Standard** | `db_standard` | `social_standard.yaml` | `scripts/setup/setup_standard_data.sh` | Separate node + edge tables |
| **FK-edge** | `db_fk_edge` | `orders_customers_fk.yaml` | `scripts/setup/setup_fk_edge_data.sh` | FK column as relationship |
| **Denormalized** | `db_denormalized` | `flights_denormalized.yaml` | `scripts/setup/setup_denormalized_data.sh` | Node properties embedded in edge table |
| **Polymorphic** | `db_polymorphic` | `social_polymorphic.yaml` | `scripts/setup/setup_polymorphic_data.sh` | Single table, type_column discriminator |
| **Composite ID** | `db_composite_id` | `schemas/examples/composite_node_id_test.yaml` | `scripts/setup/setup_composite_id_data.sh` | Multi-column node identity |

## Other Development Schemas

- `social_dev.yaml` - Extended social network schema with test-only additions (uses `brahmand` database):
  - ZeekLog node (for UNWIND array column testing)
  - PatternCompUser node (for pattern comprehension testing)
  
## Usage

```bash
# Browser testing with isolated databases
export GRAPH_CONFIG_PATH="./schemas/dev/social_standard.yaml"
export GRAPH_CONFIG_PATH="./schemas/dev/orders_customers_fk.yaml"
export GRAPH_CONFIG_PATH="./schemas/dev/flights_denormalized.yaml"
export GRAPH_CONFIG_PATH="./schemas/dev/social_polymorphic.yaml"

# Legacy development schema (brahmand database)
export GRAPH_CONFIG_PATH="./schemas/dev/social_dev.yaml"

# Benchmarks
export GRAPH_CONFIG_PATH="./benchmarks/social_network/schemas/social_benchmark.yaml"
```

**Important**: Keep benchmark schemas pristine! All test-specific schema modifications should go here.
