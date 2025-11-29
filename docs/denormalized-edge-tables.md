# Denormalized Edge Tables Guide

This guide explains how to use ClickGraph with denormalized edge tables where node properties are embedded directly in the relationship table.

## Overview

In many real-world datasets, especially log data, the graph structure is implicit in a single table. For example:
- **Network logs**: Source IP → Destination IP connections
- **Flight data**: Origin Airport → Destination Airport flights
- **Event logs**: Actor → Target interactions

Instead of joining separate node tables, all properties are already in the edge table. ClickGraph supports this pattern through **denormalized node properties**.

## Quick Start

### Example: Network Connection Logs (Zeek conn.log)

Your ClickHouse table:
```sql
CREATE TABLE conn_log (
    ts Float64,
    uid String,
    orig_h String,        -- Source IP (node ID)
    orig_p UInt16,        -- Source port
    resp_h String,        -- Dest IP (node ID)  
    resp_p UInt16,        -- Dest port
    proto String,
    service String,
    duration Float64
) ENGINE = MergeTree() ORDER BY (ts, uid)
```

Schema configuration:
```yaml
name: network_logs
version: "1.0"

graph_schema:
  nodes:
    - label: IP
      database: zeek
      table: conn_log        # Same table as the edge!
      id_column: ip
      property_mappings: {}  # Empty - properties are denormalized
      
      # Properties when IP is the source
      from_node_properties:
        ip: orig_h
        port: orig_p
      
      # Properties when IP is the destination
      to_node_properties:
        ip: resp_h
        port: resp_p

  relationships:
    - type: ACCESSED
      database: zeek
      table: conn_log
      from_id: orig_h
      to_id: resp_h
      from_node: IP
      to_node: IP
      edge_id: uid          # Unique connection ID
      
      property_mappings:
        timestamp: ts
        protocol: proto
        service: service
        duration: duration
```

Query examples:
```cypher
-- Find all connections from an IP
MATCH (src:IP)-[r:ACCESSED]->(dst:IP)
WHERE src.ip = '192.168.4.76'
RETURN src.ip, dst.ip, r.service

-- Count connections by protocol
MATCH ()-[r:ACCESSED]->()
RETURN r.protocol, count(*) as cnt
ORDER BY cnt DESC

-- Find all unique IPs in the network
MATCH (ip:IP)
RETURN DISTINCT ip.ip
```

## How It Works

### Traditional vs Denormalized

**Traditional (Normalized)**:
- Separate `airports` table with airport properties
- Separate `flights` table with flight properties
- Queries require JOINs between tables

**Denormalized**:
- Single `flights` table with ALL data
- Airport properties embedded (OriginCity, DestCity, etc.)
- No JOINs needed - 10-100x faster queries!

### The Key Insight

When you define:
```yaml
nodes:
  - label: IP
    table: conn_log         # Points to edge table
    from_node_properties:
      ip: orig_h
    to_node_properties:
      ip: resp_h
```

ClickGraph understands that:
1. `IP` nodes don't have their own physical table
2. When IP appears as source, use `orig_h` column
3. When IP appears as target, use `resp_h` column
4. Generate UNION queries for node-only patterns

## Schema Configuration

### Required Fields for Denormalized Nodes

```yaml
nodes:
  - label: NodeLabel
    database: your_db
    table: edge_table_name      # Must match relationship table!
    id_column: logical_id_name
    property_mappings: {}        # Usually empty
    
    from_node_properties:        # Required for denormalized
      property_name: column_name
    
    to_node_properties:          # Required for denormalized
      property_name: column_name
```

### Relationship Configuration

```yaml
relationships:
  - type: RELATIONSHIP_TYPE
    database: your_db
    table: edge_table_name
    from_id: source_id_column
    to_id: target_id_column
    from_node: NodeLabel
    to_node: NodeLabel
    
    edge_id: unique_id_column    # Optional but recommended
    
    property_mappings:
      cypher_property: column_name
```

## Use Cases

### 1. Network Logs (Zeek/Bro)

**conn.log** - Connection records:
```yaml
# IP -[ACCESSED]-> IP
from_node_properties: { ip: orig_h }
to_node_properties: { ip: resp_h }
```

**dns.log** - DNS queries:
```yaml
# IP -[REQUESTED]-> Domain
from_node_properties: { ip: orig_h }
to_node_properties: { name: query }
```

### 2. Flight Data (OnTime)

```yaml
# Airport -[FLIGHT]-> Airport
from_node_properties:
  code: Origin
  city: OriginCityName
  state: OriginState
to_node_properties:
  code: Dest
  city: DestCityName
  state: DestState
```

### 3. Social/Activity Logs

```yaml
# User -[INTERACTED]-> User
from_node_properties:
  user_id: actor_id
  name: actor_name
to_node_properties:
  user_id: target_id
  name: target_name
```

## Advanced Patterns

### Coupled Edges (Multi-Hop on Same Table)

When multiple relationships share the same physical table AND connect through a common "coupling node", ClickGraph automatically optimizes queries by:
1. **Skipping unnecessary JOINs** - No self-join on the same table row
2. **Unifying table aliases** - All relationships use a single table alias

This is common in log data where one row represents multiple graph relationships.

**Example: DNS Log with Coupled Edges**

Consider a DNS log where each row contains:
- Source IP → Domain (REQUESTED relationship)
- Domain → Resolved IPs (RESOLVED_TO relationship)

```yaml
# Schema: Two relationship types from ONE table row
edges:
  - type: REQUESTED
    table: dns_log
    from_id: "id.orig_h"    # Source IP
    to_id: query             # Domain (coupling node)
    from_node: IP
    to_node: Domain
    
  - type: RESOLVED_TO
    table: dns_log
    from_id: query           # Domain (coupling node)  
    to_id: answers           # Resolved IP
    from_node: Domain
    to_node: ResolvedIP
```

**Cypher Query**:
```cypher
MATCH (ip:IP)-[r1:REQUESTED]->(d:Domain)-[r2:RESOLVED_TO]->(rip:ResolvedIP)
WHERE ip.ip = '192.168.4.76'
RETURN ip.ip, d.name, rip.ips
```

**Generated SQL** (optimized - NO self-join!):
```sql
SELECT 
  r1."id.orig_h" AS "ip.ip",
  r1.query AS "d.name",
  r1.answers AS "rip.ips"
FROM zeek.dns_log AS r1
WHERE r1."id.orig_h" = '192.168.4.76'
```

Notice: Both `r1` and `r2` use the same alias `r1` because they're coupled through the Domain node (`d`).

**When Edges Are Coupled**:
- ✅ Same physical table
- ✅ Share a "coupling node" (e.g., `r1.to_node = r2.from_node`)
- ✅ Relationship chain is sequential (no branching)

**Benefits**:
- 10-100x faster queries (no self-joins)
- Simpler generated SQL
- Works with all Cypher features: WHERE, aggregations, ORDER BY, UNWIND

### Multiple Relationship Types from One Table

If your table has different relationship types:

```yaml
relationships:
  - type: LIKES
    table: interactions
    from_id: actor_id
    to_id: target_id
    # Add filter for type column if needed
    
  - type: FOLLOWS
    table: interactions
    from_id: actor_id
    to_id: target_id
```

### UNWIND with Coupled Edges

When your denormalized table contains array columns (like DNS `answers`), use UNWIND to flatten them:

```cypher
MATCH (ip:IP)-[:REQUESTED]->(d:Domain)-[:RESOLVED_TO]->(rip:ResolvedIP)
WHERE ip.ip = '192.168.4.76'
UNWIND rip.ips AS resolved_ip
RETURN ip.ip, d.name, resolved_ip
```

**Generated SQL**:
```sql
SELECT 
  r1."id.orig_h" AS "ip.ip",
  r1.query AS "d.name",
  resolved_ip AS "resolved_ip"
FROM zeek.dns_log AS r1
ARRAY JOIN r1.answers AS resolved_ip
WHERE r1."id.orig_h" = '192.168.4.76'
```

The property name `rip.ips` is automatically mapped to the correct SQL column (`answers`) based on the schema's `to_node_properties` definition.

### Node-Only Queries (UNION Pattern)

When you query just nodes without relationships:
```cypher
MATCH (ip:IP) RETURN DISTINCT ip.ip
```

ClickGraph automatically generates UNION:
```sql
SELECT DISTINCT orig_h AS "ip.ip" FROM conn_log
UNION DISTINCT
SELECT DISTINCT resp_h AS "ip.ip" FROM conn_log
```

This finds ALL IPs whether they appear as source or destination.

## Performance Benefits

| Query Type | Traditional (JOINs) | Denormalized |
|------------|--------------------:|-------------:|
| Simple traversal | ~100ms | ~10ms |
| Multi-hop path | ~1000ms | ~50ms |
| Aggregation | ~500ms | ~20ms |

*Benchmarks on 10M row dataset*

## Limitations

1. **Array columns**: Columns containing arrays (e.g., `answers: ["ip1", "ip2"]`) require special handling (coming soon)

2. **Property name conflicts**: If same property name exists in both `from_node_properties` and `to_node_properties`, you must use unique Cypher property names

3. **Multi-table denormalization**: Currently supports one edge table per node type. For nodes spread across multiple edge tables, use UNION views.

## Troubleshooting

### "Property not found" errors

Check that:
1. Property is defined in `from_node_properties` OR `to_node_properties`
2. Column name matches actual table column
3. Cypher property name matches schema definition

### Wrong results for node-only queries

Verify:
1. Both `from_node_properties` and `to_node_properties` are defined
2. The ID property is mapped in both (e.g., `ip: orig_h` and `ip: resp_h`)

### SQL shows JOINs when expecting none

The node table must point to the same table as the relationship:
```yaml
nodes:
  - label: IP
    table: conn_log      # <-- Must match relationship table
relationships:
  - type: ACCESSED
    table: conn_log      # <-- Same table
```

### Coupled edges still generating JOINs

For coupled edge optimization to work:
1. Both relationships must be on the **exact same table** (same database.table)
2. They must share a **coupling node** (e.g., `r1.to_node = r2.from_node`)
3. The pattern must be **sequential** (linear chain, not branching)

Example of a pattern that WON'T be coupled:
```cypher
-- Branching pattern - not coupled
MATCH (a)-[r1]->(b), (a)-[r2]->(c)
```

## Example Schemas

See the `schemas/examples/` directory for complete examples:
- `zeek_conn_log.yaml` - Network connection logs
- `zeek_dns_log.yaml` - DNS query logs (includes array handling)
- `ontime_denormalized.yaml` - Flight data

## Related Documentation

- [Cypher Language Reference](./Cypher-Language-Reference.md)
- [Schema Configuration Guide](./Schema-Configuration.md)
- [Array Flattening Design](../notes/array-flattening-design.md)
