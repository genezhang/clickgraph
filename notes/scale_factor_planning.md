# Scale Factor Definition and Data Size Planning

**Date**: November 12, 2025  
**Script**: `setup_benchmark_unified.py`

---

## Scale Factor Formulas

The unified benchmark uses these exact formulas based on **realistic social network statistics**:

```python
num_users   = scale_factor * 1000
num_follows = scale_factor * 100000   # 100 follows per user (realistic median)
num_posts   = scale_factor * 20000    # 20 posts per user (~6 months activity)
```

**Rationale**:
- **100 follows/user**: Matches Twitter/Instagram median active users (~100-150)
- **20 posts/user**: Represents ~6 months of activity for active users (~3-4 posts/month)
- Based on public social network statistics (Twitter, Instagram, LinkedIn)

**Why these ratios?**:
- 71% of benchmark queries (10/14) use FOLLOWS relationships heavily
- 2 new queries (12.5%) use Posts to test content patterns
- Multi-hop and variable-length path queries need sufficient graph density
- Lower post ratio (20 vs 50) reduces data volume while remaining realistic

---

## Complete Scale Factor Table

| Scale Factor | Users | Follows | Posts | Total Rows | Use Case | Est. Time |
|--------------|-------|---------|-------|------------|----------|-----------|
| **1** | 1,000 | 100,000 | 20,000 | 121,000 | Dev testing, quick validation | ~2 sec |
| **5** | 5,000 | 500,000 | 100,000 | 605,000 | Small integration tests | ~10 sec |
| **10** | 10,000 | 1,000,000 | 200,000 | 1,210,000 | Medium integration | ~20 sec |
| **20** | 20,000 | 2,000,000 | 400,000 | 2,420,000 | Larger integration | ~40 sec |
| **50** | 50,000 | 5,000,000 | 1,000,000 | 6,050,000 | Moderate stress test | ~1.5 min |
| **100** | 100,000 | 10,000,000 | 2,000,000 | 12,100,000 | Large dataset | ~3 min |
| **200** | 200,000 | 20,000,000 | 4,000,000 | 24,200,000 | Large stress test | ~6 min |
| **500** | 500,000 | 50,000,000 | 10,000,000 | 60,500,000 | Production-like | ~15 min |
| **1000** | 1,000,000 | 100,000,000 | 20,000,000 | 121,000,000 | Production scale | ~30 min |
| **2000** | 2,000,000 | 200,000,000 | 40,000,000 | 242,000,000 | Large production | ~1 hour |
| **5000** | 5,000,000 | 500,000,000 | 100,000,000 | 605,000,000 | Enterprise scale | ~2.5 hours |
| **10000** | 10,000,000 | 1,000,000,000 | 200,000,000 | 1,210,000,000 | Ultra-large (1B+ rows!) | ~5 hours |

**Note**: With 100:20 ratios, scale=10000 gives **10M users, 1B follows, 200M posts** - over 1.2 billion total rows!

---

## Table Engine Selection

The benchmark supports two ClickHouse table engines:

### Memory Engine (Default for scale ≤ 100)

```bash
python setup_benchmark_unified.py --scale 10 --engine Memory
```

**Pros**:
- ✅ **Fastest** - No disk I/O, pure memory operations
- ✅ **Simple** - No configuration needed
- ✅ **Works on Windows Docker** - No volume permission issues

**Cons**:
- ❌ **Non-persistent** - Data lost on server restart
- ❌ **RAM limited** - All data in memory (scale=100 = ~1.2GB, scale=1000 = ~12GB)
- ❌ **No compression** - Wastes memory
- ❌ **No indexing** - Can't optimize queries with PRIMARY KEY
- ❌ **Not production-like** - Nobody runs production on Memory engine

**Recommended for**: scale 1-100 (dev/integration testing, quick validation)

### MergeTree Engine (Recommended for scale ≥ 100)

```bash
python setup_benchmark_unified.py --scale 1000 --engine MergeTree
```

**Pros**:
- ✅ **Persistent** - Survives server restarts
- ✅ **Compressed** - LZ4 compression reduces disk usage 5-10x
- ✅ **Indexed** - ORDER BY creates sorting key for fast lookups
- ✅ **Scalable** - Handles billions of rows efficiently
- ✅ **Production-ready** - Same engine used in real deployments
- ✅ **Better performance** - Indexing accelerates WHERE clauses and JOINs

**Cons**:
- ⚠️ **Disk I/O** - Slightly slower inserts than Memory
- ⚠️ **Windows Docker caveat** - May require volume mount permissions (see workaround below)

**Recommended for**: scale 100+ (stress testing, production-scale validation)

### Windows Docker Workaround for MergeTree

If you encounter permission errors on Windows with MergeTree:

```bash
# Option 1: Run ClickHouse with elevated permissions
docker exec -it --user root clickhouse chmod 777 /var/lib/clickhouse

# Option 2: Use named volume instead of bind mount
docker volume create clickhouse_data
# Update docker-compose.yaml to use named volume

# Option 3: Stick with Memory engine for testing
python setup_benchmark_unified.py --scale 100 --engine Memory
```

---

## Engine Recommendation by Scale

| Scale | Total Rows | RAM (Memory) | Disk (MergeTree) | Recommended Engine | Reason |
|-------|------------|--------------|------------------|-------------------|--------|
| 1 | 121K | ~12 MB | ~2 MB | **Memory** | Instant, negligible RAM |
| 10 | 1.2M | ~120 MB | ~20 MB | **Memory** | Fast, acceptable RAM |
| 100 | 12M | ~1.2 GB | ~200 MB | **Memory or MergeTree** | Borderline - Memory if enough RAM |
| 1000 | 121M | ~12 GB | ~2 GB | **MergeTree** | Large RAM footprint |
| 5000 | 605M | ~60 GB | ~10 GB | **MergeTree** | Exceeds typical RAM |
| 10000 | 1.2B | ~120 GB | ~20 GB | **MergeTree** | Far exceeds typical RAM |

**Rule of thumb**: 
- Scale ≤ 100: Use Memory (fast, simple)
- Scale 100-1000: Use MergeTree if testing persistence/compression, Memory if RAM available
- Scale ≥ 1000: **Must use MergeTree** (Memory engine impractical)

---

## Recommended Standard Scales

For consistent benchmarking, we recommend these **4 standard scales**:

### 1. Small (scale=1)
- **Users**: 1,000
- **Follows**: 100,000 (100 follows per user avg)
- **Posts**: 20,000 (20 posts per user avg)
- **Total**: 121,000 rows
- **Purpose**: Quick validation, dev testing, CI/CD
- **Time**: ~2 seconds

### 2. Medium (scale=10)
- **Users**: 10,000
- **Follows**: 1,000,000 (100 follows per user avg)
- **Posts**: 200,000 (20 posts per user avg)
- **Total**: 1,210,000 rows
- **Purpose**: Integration testing, feature validation
- **Time**: ~20 seconds

### 3. Large (scale=100)
- **Users**: 100,000
- **Follows**: 10,000,000 (100 follows per user avg)
- **Posts**: 2,000,000 (20 posts per user avg)
- **Total**: 12,100,000 rows
- **Purpose**: Stress testing, performance regression detection
- **Time**: ~3 minutes

### 4. XLarge (scale=1000)
- **Users**: 1,000,000
- **Follows**: 100,000,000 (100 follows per user avg)
- **Posts**: 20,000,000 (20 posts per user avg)
- **Total**: 121,000,000 rows
- **Purpose**: Production-scale validation, capacity planning
- **Time**: ~30 minutes

### Optional: XXLarge (scale=5000)
- **Users**: 5,000,000
- **Follows**: 500,000,000 (100 follows per user avg)
- **Posts**: 100,000,000 (20 posts per user avg)
- **Total**: 605,000,000 rows
- **Purpose**: Enterprise-scale testing, maximum capacity
- **Time**: ~2.5 hours

---

## Real-World Social Network Statistics

Our ratios (1\:100\:50) are based on public statistics from major social networks:

### Average Follows/Followers per User

| Platform | Average Follows | Median | Notes |
|----------|----------------|--------|-------|
| **Twitter/X** | ~700 | ~100 | Heavily skewed (top 1% has 10K+) |
| **Instagram** | ~300 | ~100-150 | Following count |
| **LinkedIn** | ~500 | ~200 | Professional network |
| **Facebook** | ~338 | ~200 | Friends (bidirectional) |

**Our choice: 100** - Represents active median user across platforms

### Average Posts per User

| Platform | Posts/Month (Active) | Posts/Year | Notes |
|----------|---------------------|------------|-------|
| **Twitter/X** | ~30 | ~360 | Active tweeters |
| **Instagram** | ~5-10 | ~60-120 | Photo-sharing platform |
| **LinkedIn** | ~2-4 | ~24-48 | Professional updates |
| **Facebook** | ~10-15 | ~120-180 | Mixed content |

**Our choice: 20** - Represents ~6 months of activity for active users (~3-4 posts/month)

### Benchmark Query Usage Analysis

**Queries using FOLLOWS**: **10 out of 16** (62.5%)
- direct_relationships
- multi_hop_2  
- friends_of_friends
- variable_length_exact_2
- variable_length_range_1to3
- shortest_path
- aggregation_follower_count
- mutual_follows
- param_filter_function (indirectly)
- param_variable_path

**Queries using Posts**: **2 out of 16** (12.5%)
- user_post_count: Count posts per user, rank by activity
- active_users_followers: Find users with >3x avg posts, show their follower count

**Conclusion**: 
- FOLLOWS relationships remain the primary stress test (62.5% of queries)
- Post queries added to test content patterns and mixed aggregations
- Lower post ratio (20 vs 50) reduces data volume by 60% while remaining realistic

---

## Data Density Analysis

### Follows per User Distribution

With **5,000 follows** for **1,000 users** (scale=1):
- **Average**: 5 follows per user
- **Distribution**: Random (Poisson-like)
- **Min**: 0 follows (some users)
- **Max**: ~15+ follows (popular users)
- **Formula**: `follower_id = rand() % num_users`

This creates a **realistic social network** with:
- Some users with no followers
- Most users with 3-7 followers
- A few "influencers" with 10+ followers

### Posts per User Distribution

With **2,000 posts** for **1,000 users** (scale=1):
- **Average**: 2 posts per user
- **Distribution**: Random (Poisson-like)
- **Min**: 0 posts (lurkers)
- **Max**: ~8+ posts (active users)
- **Formula**: `author_id = rand() % num_users`

This creates:
- Lurkers (no posts)
- Casual users (1-2 posts)
- Active users (3-5 posts)
- Power users (6+ posts)

---

## Scaling Characteristics

### Linear Scaling
All metrics scale **linearly** with scale factor:
- Users: O(scale_factor)
- Follows: O(scale_factor)
- Posts: O(scale_factor)
- Total rows: O(scale_factor)

### Performance Implications

**ClickHouse Memory Engine**:
- Scale 1-100: Very fast (<1-20 sec)
- Scale 100-1000: Fast (~20 sec - 3 min)
- Scale 1000-5000: Moderate (~3-10 min)
- Scale 5000+: Requires careful tuning

**Query Performance** (expected):
- Simple lookups: O(log n) with proper indexing
- Traversals: O(k * edges) where k = hop count
- Variable-length paths: O(depth * edges)
- Aggregations: O(n) scan with filtering

### Memory Usage Estimates

Approximate memory usage for Memory engine:

| Scale | Total Rows | Est. Memory | CH Recommended RAM |
|-------|------------|-------------|-------------------|
| 1 | 8K | <1 MB | 512 MB |
| 10 | 80K | ~10 MB | 1 GB |
| 100 | 800K | ~100 MB | 2 GB |
| 1000 | 8M | ~1 GB | 4 GB |
| 5000 | 40M | ~5 GB | 16 GB |
| 10000 | 80M | ~10 GB | 32 GB |

*Note: Actual memory depends on string lengths, ClickHouse internal structures*

---

## Custom Scale Factors

You can use **any integer scale factor** for custom scenarios:

### Example: Scale=7 (7K users)
```bash
python setup_benchmark_unified.py --scale 7
```
- Users: 7,000
- Follows: 35,000
- Posts: 14,000
- Total: 56,000 rows

### Example: Scale=250 (250K users)
```bash
python setup_benchmark_unified.py --scale 250
```
- Users: 250,000
- Follows: 1,250,000
- Posts: 500,000
- Total: 1,950,000 rows

### Example: Scale=3500 (3.5M users)
```bash
python setup_benchmark_unified.py --scale 3500
```
- Users: 3,500,000
- Follows: 17,500,000
- Posts: 7,000,000
- Total: 28,000,000 rows

---

## Comparison with Historical Benchmarks

### Old Approach (Manual sizes)
```
Small:  1,000 users,    4,997 follows,  2,000 posts  (inconsistent)
Medium: 10,000 users,   50,000 follows, 5,000 posts  (follows:users = 5:1, posts different)
Large:  5,000,000 users, 50,000,000 follows, 25,000,000 posts (10:1 ratio)
```

### New Unified Approach (Consistent ratios)
```
Scale=1:    1,000 users,    5,000 follows,     2,000 posts  (5:1 follows, 2:1 posts)
Scale=10:   10,000 users,   50,000 follows,    20,000 posts (5:1 follows, 2:1 posts)
Scale=5000: 5,000,000 users, 25,000,000 follows, 10,000,000 posts (5:1 follows, 2:1 posts)
```

**Benefits**:
- ✅ Consistent ratios across all scales
- ✅ Predictable scaling behavior
- ✅ Easier to reason about performance
- ✅ No special cases or manual adjustments

---

## Choosing the Right Scale

### Decision Matrix

| If you want to... | Use Scale | Why |
|-------------------|-----------|-----|
| Quick smoke test (<10 sec) | 1-5 | Instant feedback |
| Integration test (30 sec) | 10-20 | Realistic but fast |
| Performance regression test | 100 | Catches slowdowns |
| Capacity planning | 1000 | Matches prod scale |
| Stress test / find limits | 5000+ | Push boundaries |
| Custom scenario | X | Flexibility |

### Development Workflow
```bash
# 1. During development: Quick validation
python setup_benchmark_unified.py --scale 1
python test_benchmark_suite.py --scale 1

# 2. Before commit: Integration test
python setup_benchmark_unified.py --scale 10
python test_benchmark_suite.py --scale 10 --iterations 3

# 3. Before release: Full benchmark
for scale in 1 10 100 1000; do
    python setup_benchmark_unified.py --scale $scale
    python test_benchmark_suite.py --scale $scale --iterations 5 --output results_${scale}.json
done
```

---

## Future Extensions

### Possible Additional Multipliers

If needed, we could add more table types:

```python
num_users = scale_factor * 1000
num_follows = scale_factor * 5000       # 5x (existing)
num_posts = scale_factor * 2000         # 2x (existing)
num_likes = scale_factor * 10000        # 10x (future)
num_comments = scale_factor * 3000      # 3x (future)
num_shares = scale_factor * 1000        # 1x (future)
```

### Variable Multipliers

For more control:
```bash
python setup_benchmark_unified.py --scale 100 --follows-multiplier 10 --posts-multiplier 5
```

This would allow:
- Scale=100, follows=10x → 1M follows (denser social graph)
- Scale=100, posts=5x → 500K posts (more active users)

---

## Summary

**Current Formula**:
```
Users   = scale_factor × 1,000
Follows = scale_factor × 5,000  (5:1 ratio)
Posts   = scale_factor × 2,000  (2:1 ratio)
```

**Standard Scales**: 1, 10, 100, 1000 (covers 3 orders of magnitude)

**Characteristics**:
- Linear scaling (predictable)
- Consistent ratios (5 follows, 2 posts per user avg)
- Realistic distribution (Poisson-like randomness)
- Fast generation (ClickHouse native functions)
- Flexible (any integer scale factor)

This design balances **simplicity** (easy to understand), **flexibility** (any scale), and **realism** (typical social network patterns).



