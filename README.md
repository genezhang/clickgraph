<div align="center">
  <img src="./logo.svg" height="200">
</div>

# Brahmand

#### Brahmand (Sanskrit for Universe)

**A high-performance, stateless graph-analysis layer for ClickHouse.**

> **Note:Brahmand is under active development and not yet production-ready. Some Cypher features are still missing.**


---

## Features

- **ClickHouse-native**  
  Extends ClickHouse with native graph modeling, merging OLAP speed with graph-analysis power.  
- **Stateless**  
  Offloads all storage and query execution to ClickHouse—no extra datastore.  
- **Cypher-query**  
  Industry-standard Cypher syntax for intuitive, expressive property-graph querying.  
- **Analytical-scale**  
  Optimized for very large datasets and complex multi-hop traversals.

---

## Architecture

Brahmand runs as a lightweight graph wrapper alongside ClickHouse:

![acrhitecture](./architecture.png)

1. **Client** sends a Cypher query to Brahmand.  
2. **Brahmand** parses & plans the query, translates to ClickHouse SQL.  
3. **ClickHouse** executes the SQL and returns results.  
4. **Brahmand** sends results back to the client.

---

## 🚀 GraphView1 Branch - View-Based Graph Analysis

The `graphview1` branch introduces a revolutionary **view-based graph model** that enables graph analysis on existing ClickHouse tables without data migration:

- **Zero Migration**: Transform existing relational data into graph format through YAML configuration
- **Native Performance**: Leverages ClickHouse's columnar storage and query optimization
- **Standard Cypher**: Full compatibility with Cypher query syntax for graph traversals
- **Production Ready**: 374/374 tests passing with comprehensive validation and error handling

**Example**: Map your `users` and `user_follows` tables to a social network graph with simple configuration:
```yaml
views:
  - name: social_network
    nodes:
      user:
        source_table: users
        id_column: user_id
        property_mappings:
          name: full_name
    relationships:
      follows:
        source_table: user_follows
        from_column: follower_id
        to_column: followed_id
```

See `docs/graphview1-branch-summary.md` for complete technical details.

---

## Docs and Installation
Check [Docs](https://www.brahmanddb.com/introduction/intro) here.


## Benchmark
Preliminary informal tests on a MacBook Pro (M3 Pro, 18 GB RAM) running Brahmand in Docker against a ~12 million-node Stack Overflow dataset show multihop traversals running approximately 10× faster than Neo4j v2025.03. These early, unoptimized results are for reference only; a full benchmark report is coming soon.

## License
Brahmand is licensed under the Apache License, Version 2.0. See the LICENSE file for details.

## Issues & Contributing
Feel free to submit issues and enhancement requests. All contributions are welcomed.