# Social Network Benchmark

This benchmark uses synthetic social network data to test graph queries on traditional node/edge table patterns.

## Data Model

### Nodes
- **User**: Users with profile information
- **Post**: User-generated content

### Edges
- **FOLLOWS**: User follows another User (many-to-many)
- **AUTHORED**: User authored a Post (one-to-many)
- **LIKED**: User liked a Post (many-to-many)
- **PURCHASED**: User purchased a Product (many-to-many, if enabled)

## Schema Pattern

This benchmark tests **traditional normalized** graph patterns:
- Separate tables for nodes (`users_bench`, `posts_bench`)
- Separate tables for edges (`user_follows_bench`, `post_likes_bench`)
- Property mappings between Cypher properties and SQL columns

## Data Setup

### Generate Data

```bash
cd benchmarks/social_network/data

# Small scale (1x) - ~10K users, ~50K follows
python3 setup_unified.py --scale 1

# Medium scale (10x) - ~100K users, ~500K follows  
python3 setup_unified.py --scale 10

# Large scale (100x) - ~1M users, ~5M follows
python3 generate_large_scale.py
python3 load_large_scale.py
```

### Schema Configuration

See `schemas/social_benchmark.yaml` for the full schema definition.

Key property mappings:
- `u.name` → `users_bench.full_name`
- `u.email` → `users_bench.email_address`

## Running Benchmarks

```bash
# Start ClickGraph server with social benchmark schema
export GRAPH_CONFIG_PATH="./benchmarks/social_network/schemas/social_benchmark.yaml"
cargo run --release --bin clickgraph

# Run benchmark suite
cd benchmarks/social_network/queries
python3 suite.py
```

## Query Categories

### Basic Queries (`suite.py`)
- Single node lookups
- Property filtering
- Simple traversals

### Medium Complexity (`medium.py`)
- Multi-hop traversals
- Aggregations
- Friend-of-friend patterns

### Advanced Queries (`final.py`)
- Variable-length paths
- Complex aggregations
- Pattern matching

## Example Queries

```cypher
-- Find users and their follower count
MATCH (u:User)<-[:FOLLOWS]-(follower:User)
RETURN u.name, count(follower) as followers
ORDER BY followers DESC
LIMIT 10

-- Friend of friend recommendations
MATCH (u:User {user_id: 1})-[:FOLLOWS]->(friend)-[:FOLLOWS]->(fof:User)
WHERE NOT (u)-[:FOLLOWS]->(fof) AND u <> fof
RETURN fof.name, count(friend) as mutual_friends
ORDER BY mutual_friends DESC
LIMIT 5

-- Posts liked by users I follow
MATCH (me:User {user_id: 1})-[:FOLLOWS]->(friend)-[:LIKED]->(p:Post)
RETURN p.title, count(friend) as friend_likes
ORDER BY friend_likes DESC
```

## Notes

- This benchmark tests **traditional normalized** table patterns
- Good for testing JOIN generation between separate tables
- Tests property mapping resolution (Cypher property → SQL column)
- Default schema for development and testing
