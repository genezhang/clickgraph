# ClickGraph vs Other Graph Analytics Solutions

This document provides an objective comparison between ClickGraph and other graph query solutions for analytical workloads.

## Quick Summary

| Solution | Best For | Query Language | Deployment | Cost Model |
|----------|----------|----------------|------------|------------|
| **ClickGraph** | ClickHouse users needing graph queries | Cypher | Self-hosted | Open source |
| **PuppyGraph** | Multi-source data lake graph queries | Gremlin, Cypher | Managed SaaS | Enterprise |
| **TigerGraph** | Large-scale enterprise graph analytics | GSQL | Self-hosted/Cloud | Enterprise |
| **NebulaGraph** | Distributed graph database | nGQL, Cypher | Self-hosted | Open source |

---

## Detailed Comparison

### Architecture & Data Access

| Feature | ClickGraph | PuppyGraph | TigerGraph | NebulaGraph |
|---------|------------|------------|------------|-------------|
| **Architecture** | Query translator | Query federation | Native graph DB | Native graph DB |
| **Data storage** | ClickHouse tables | Connectors to sources | Native storage | Native storage |
| **ClickHouse support** | ✅ Native | ✅ Connector | ❌ ETL required | ❌ ETL required |
| **Iceberg/Parquet** | Via ClickHouse | ✅ Native connectors | ❌ ETL required | ❌ ETL required |
| **Data movement** | None (views) | None (connectors) | ETL required | ETL required |
| **Schema definition** | YAML mapping | UI/Config | Schema DDL | Schema DDL |

### Query Languages

| Feature | ClickGraph | PuppyGraph | TigerGraph | NebulaGraph |
|---------|------------|------------|------------|-------------|
| **Cypher** | ✅ Primary | ✅ Supported | ❌ | ✅ Partial |
| **Gremlin** | ❌ | ✅ Primary | ❌ | ❌ |
| **Proprietary** | - | - | GSQL | nGQL |
| **Learning curve** | Low (Cypher) | Moderate | Steep (GSQL) | Moderate |
| **Neo4j compatibility** | High | High | None | Partial |

### Analytical Capabilities

This is where ClickGraph differentiates - direct access to ClickHouse's analytical functions.

| Feature | ClickGraph | PuppyGraph | TigerGraph | NebulaGraph |
|---------|------------|------------|------------|-------------|
| **HyperLogLog unique counts** | ✅ `ch.uniq()` | ❌ | ❌ | ❌ |
| **Quantiles (15+ variants)** | ✅ `ch.quantile*()` | ❌ | Limited | ❌ |
| **Funnel analysis** | ✅ `ch.windowFunnel()` | ❌ | Custom GSQL | ❌ |
| **Retention analysis** | ✅ `ch.retention()` | ❌ | Custom GSQL | ❌ |
| **Sequence matching** | ✅ `ch.sequenceMatch()` | ❌ | Custom GSQL | ❌ |
| **TopK with weights** | ✅ `ch.topKWeighted()` | ❌ | ❌ | ❌ |
| **JSON extraction** | ✅ `ch.JSONExtract*()` | SQL passthrough | Limited | Limited |
| **Geo/H3 functions** | ✅ `ch.geoToH3()` | SQL passthrough | Limited | ❌ |
| **Custom aggregates** | ✅ `chagg.*` prefix | ❌ | UDF support | UDF support |
| **500+ CH functions** | ✅ Full access | ❌ | ❌ | ❌ |

### Graph Features

| Feature | ClickGraph | PuppyGraph | TigerGraph | NebulaGraph |
|---------|------------|------------|------------|-------------|
| **Multi-hop traversals** | ✅ | ✅ | ✅ | ✅ |
| **Variable-length paths** | ✅ | ✅ | ✅ | ✅ |
| **Shortest path** | ✅ | ✅ | ✅ | ✅ |
| **PageRank** | ✅ | ✅ | ✅ | ✅ |
| **OPTIONAL MATCH** | ✅ | ✅ | N/A | Partial |
| **Graph algorithms** | Basic | Basic | Extensive | Moderate |
| **Real-time updates** | ✅ (via CH) | Depends on source | ✅ | ✅ |

### Deployment & Operations

| Feature | ClickGraph | PuppyGraph | TigerGraph | NebulaGraph |
|---------|------------|------------|------------|-------------|
| **Deployment model** | Self-hosted | Managed SaaS | Self-hosted/Cloud | Self-hosted |
| **Complexity** | Low (single binary) | Low (managed) | High (cluster) | Moderate (cluster) |
| **Dependencies** | ClickHouse only | Multiple connectors | Standalone | Standalone |
| **Scaling** | Via ClickHouse | Managed | Manual/Auto | Manual |
| **High availability** | Via ClickHouse | Managed | Built-in | Built-in |

### Cost & Licensing

| Feature | ClickGraph | PuppyGraph | TigerGraph | NebulaGraph |
|---------|------------|------------|------------|-------------|
| **License** | MIT (open source) | Proprietary | Proprietary | Apache 2.0 |
| **Cost** | Free | Enterprise pricing | Enterprise pricing | Free (community) |
| **Support** | Community | Commercial | Commercial | Community/Commercial |
| **Vendor lock-in** | Low | Medium | High (GSQL) | Low |

---

## When to Choose Each Solution

### Choose ClickGraph When:

- ✅ You already use ClickHouse for analytics
- ✅ You want graph queries without data movement
- ✅ You need ClickHouse's analytical functions (quantiles, funnels, HLL)
- ✅ You prefer Cypher and Neo4j compatibility
- ✅ You want open source with no licensing costs
- ✅ You value operational simplicity (single binary)

### Choose PuppyGraph When:

- ✅ You have data across multiple sources (Snowflake, Databricks, etc.)
- ✅ You need a managed service with minimal ops
- ✅ You prefer Gremlin or need both Gremlin and Cypher
- ✅ Budget allows for enterprise SaaS pricing
- ✅ You don't need deep ClickHouse-specific analytics

### Choose TigerGraph When:

- ✅ You need advanced graph algorithms at scale
- ✅ You're building a dedicated graph analytics platform
- ✅ Your team can learn GSQL (proprietary language)
- ✅ You need enterprise support and SLAs
- ✅ You're okay with data ETL into TigerGraph

### Choose NebulaGraph When:

- ✅ You need a distributed native graph database
- ✅ You want open source with commercial support option
- ✅ You prefer nGQL or partial Cypher support
- ✅ You need a standalone graph solution (not on existing DWH)
- ✅ You're building a dedicated graph platform

---

## ClickGraph's Unique Value Proposition

### 1. ClickHouse-Native Analytics in Cypher

No other graph solution provides direct access to ClickHouse's 500+ functions:

```cypher
-- Funnel analysis with graph traversal
MATCH (u:User)-[:VIEWED]->(p:Product)-[:PURCHASED]->(o:Order)
RETURN u.segment,
       ch.windowFunnel(86400)(event.timestamp, 
           event.type = 'view',
           event.type = 'cart', 
           event.type = 'purchase') AS funnel_stage,
       ch.uniqExact(u.user_id) AS unique_users

-- HyperLogLog + graph pattern
MATCH (u:User)-[:FOLLOWS*1..3]->(influencer:User)
WHERE influencer.followers > 10000
RETURN influencer.name, ch.uniq(u.user_id) AS reach_estimate

-- Quantile analysis on graph results
MATCH (u:User)-[r:TRANSACTION]->(m:Merchant)
RETURN m.category,
       ch.quantiles(0.5, 0.9, 0.99)(r.amount) AS amount_distribution
```

### 2. Zero Data Movement

If your data is in ClickHouse, ClickGraph queries it directly via views - no ETL, no sync, no staleness.

### 3. Open Source & Simple

- MIT license - free forever
- Single binary deployment
- No cluster management
- Your ClickHouse handles scaling

---

## Feature Roadmap Comparison

| Feature | ClickGraph | PuppyGraph | TigerGraph | NebulaGraph |
|---------|------------|------------|------------|-------------|
| **Graph neural networks** | Planned | Unknown | Available | Planned |
| **Vector similarity** | ✅ Available | Unknown | Limited | Planned |
| **More algorithms** | Planned | Continuous | Extensive | Continuous |
| **Write support** | Out of scope | Limited | Full | Full |

---

## Summary

**ClickGraph is not trying to be a general-purpose graph database.** 

It's a specialized tool for organizations that:
1. Already have data in ClickHouse
2. Want graph query capabilities on that data
3. Need ClickHouse's analytical superpowers in their graph queries
4. Prefer open source and operational simplicity

For dedicated graph database needs with full write support and extensive algorithms, TigerGraph or NebulaGraph may be better fits. For multi-source data federation, PuppyGraph excels.

**ClickGraph's niche: Graph queries + ClickHouse analytics, unified.**
