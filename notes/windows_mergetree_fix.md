# Windows MergeTree Fix Guide

**Problem**: ClickHouse MergeTree tables fail on Windows Docker with permission errors:
```
Code: 243. DB::Exception: Cannot open file /var/lib/clickhouse/data/brahmand/users_bench/...
Permission denied
```

**Root cause**: Windows bind mount (`./clickhouse_data:/var/lib/clickhouse`) doesn't preserve Linux file permissions.

---

## Solution 1: Use Named Volume (RECOMMENDED) ✅

Replace bind mount with Docker named volume - no permission issues!

**Edit `docker-compose.yaml`**:

```yaml
services:
  clickhouse-service:
    image: clickhouse/clickhouse-server:25.8.11
    container_name: clickhouse
    # ... other config ...
    volumes:
      - clickhouse_data:/var/lib/clickhouse  # Changed from ./clickhouse_data

volumes:
  clickhouse_data:  # Named volume - managed by Docker
```

**Pros**:
- ✅ No permission issues
- ✅ Better performance (no Windows filesystem overhead)
- ✅ Proper Linux permissions
- ✅ Data persists between restarts

**Cons**:
- ⚠️ Data not directly accessible on Windows filesystem
- ⚠️ Need `docker volume inspect clickhouse_data` to find location

**Test it**:
```bash
# Stop and remove existing containers
docker-compose down -v

# Start with new configuration
docker-compose up -d

# Test MergeTree tables
python benchmarks/data/setup_unified.py --scale 10 --engine MergeTree
```

---

## Solution 2: Run ClickHouse as Root ⚡

Quick fix - run ClickHouse container with root privileges.

**Edit `docker-compose.yaml`**:

```yaml
services:
  clickhouse-service:
    image: clickhouse/clickhouse-server:25.8.11
    container_name: clickhouse
    user: "0:0"  # Run as root (uid:gid = 0:0)
    # ... rest of config ...
    volumes:
      - ./clickhouse_data:/var/lib/clickhouse  # Keep bind mount
```

**Pros**:
- ✅ Simple one-line fix
- ✅ Data visible on Windows filesystem
- ✅ Works immediately

**Cons**:
- ⚠️ Security concern (not for production)
- ⚠️ ClickHouse runs as root inside container

**Test it**:
```bash
docker-compose down
docker-compose up -d
python benchmarks/data/setup_unified.py --scale 10 --engine MergeTree
```

---

## Solution 3: Fix Permissions Manually 🔧

One-time permission fix inside running container.

**Run after starting ClickHouse**:

```bash
# Start ClickHouse
docker-compose up -d

# Fix permissions (run once)
docker exec --user root clickhouse chmod -R 777 /var/lib/clickhouse
docker exec --user root clickhouse chown -R clickhouse:clickhouse /var/lib/clickhouse

# Now MergeTree works
python benchmarks/data/setup_unified.py --scale 10 --engine MergeTree
```

**Pros**:
- ✅ No docker-compose changes
- ✅ Works with bind mount
- ✅ Quick fix

**Cons**:
- ⚠️ Must rerun after `docker-compose down -v`
- ⚠️ Wide-open permissions (777)

---

## Solution 4: WSL2 Backend (Best for Development) 🐧

Use WSL2 for Docker - eliminates Windows filesystem issues entirely.

**Requirements**:
- Windows 10/11 with WSL2
- Docker Desktop with WSL2 backend enabled

**Steps**:
1. Enable WSL2 in Docker Desktop settings
2. Store project in WSL2 filesystem:
   ```bash
   # From WSL2 terminal
   cd ~
   git clone https://github.com/genezhang/clickgraph.git
   cd clickgraph
   docker-compose up -d
   ```

**Pros**:
- ✅ Native Linux filesystem (no permission issues)
- ✅ Better I/O performance
- ✅ No special configuration needed
- ✅ Production-like environment

**Cons**:
- ⚠️ Requires WSL2 setup
- ⚠️ Files not in Windows filesystem (use WSL2 file explorer)

---

## Comparison Table

| Solution | Ease | Performance | Security | Persistence | Recommended For |
|----------|------|-------------|----------|-------------|-----------------|
| **Named Volume** | ⭐⭐⭐⭐⭐ | ⭐⭐⭐⭐⭐ | ⭐⭐⭐⭐⭐ | ✅ | **Everyone** |
| **Root User** | ⭐⭐⭐⭐⭐ | ⭐⭐⭐⭐ | ⭐⭐ | ✅ | Quick testing |
| **Manual chmod** | ⭐⭐⭐ | ⭐⭐⭐⭐ | ⭐⭐ | ⚠️ Temporary | One-off tests |
| **WSL2** | ⭐⭐⭐ | ⭐⭐⭐⭐⭐ | ⭐⭐⭐⭐⭐ | ✅ | Dev environment |

---

## Our Recommendation: Solution 1 (Named Volume)

Update your `docker-compose.yaml`:

```yaml
version: '3.8'

services:
  clickhouse-service:
    image: clickhouse/clickhouse-server:25.8.11
    container_name: clickhouse
    environment:
      CLICKHOUSE_DB: "brahmand"
      CLICKHOUSE_USER: "test_user"
      CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT: "1"
      CLICKHOUSE_PASSWORD: "test_pass"
    ports:
      - "9000:9000"
      - "8123:8123"
    healthcheck:
      test: ["CMD", "clickhouse-client", "--query", "SELECT 1"]
      interval: 10s
      timeout: 5s
      retries: 5
      start_period: 30s
    volumes:
      - clickhouse_data:/var/lib/clickhouse  # ✅ Named volume

  clickgraph:
    build:
      context: .
      dockerfile: Dockerfile
    container_name: clickgraph
    depends_on:
      clickhouse-service:
        condition: service_healthy
    environment:
      CLICKHOUSE_URL: "http://clickhouse-service:8123"
      CLICKHOUSE_USER: "test_user"
      CLICKHOUSE_PASSWORD: "test_pass"
      CLICKHOUSE_DATABASE: "test_integration"
      GRAPH_CONFIG_PATH: "/app/test_integration.yaml"
    ports:
      - "8080:8080"
    volumes:
      - ./tests/integration/test_integration.yaml:/app/test_integration.yaml:ro
      - ./schemas:/app/schemas:ro

volumes:
  clickhouse_data:  # ✅ Docker-managed volume
```

**Apply it**:
```powershell
# Stop and clean
docker-compose down -v

# Start with new config
docker-compose up -d

# Wait for ClickHouse to be ready
Start-Sleep -Seconds 10

# Test MergeTree tables work!
python benchmarks/data/setup_unified.py --scale 10 --engine MergeTree
```

---

## Verify It Works

```bash
# 1. Check table engine
docker exec clickhouse clickhouse-client --query "SHOW CREATE TABLE brahmand.users_bench"

# Should see: ENGINE = MergeTree() ORDER BY user_id

# 2. Check data persists
docker exec clickhouse clickhouse-client --query "SELECT COUNT(*) FROM brahmand.users_bench"

# 3. Test restart persistence
docker-compose restart clickhouse-service
docker exec clickhouse clickhouse-client --query "SELECT COUNT(*) FROM brahmand.users_bench"

# Same count = persistence works! ✅
```

---

## Troubleshooting

**Q: Named volume - where is my data?**
```bash
docker volume inspect clickhouse_data
# Look for "Mountpoint" - that's where Docker stores it
```

**Q: Need to backup data?**
```bash
# Export data
docker exec clickhouse clickhouse-client --query "SELECT * FROM brahmand.users_bench FORMAT CSVWithNames" > backup.csv

# Or backup entire database
docker exec clickhouse clickhouse-client --query "BACKUP DATABASE brahmand TO Disk('backups', 'backup.zip')"
```

**Q: Want to clear everything and start fresh?**
```bash
docker-compose down -v  # Removes named volume
docker-compose up -d     # Creates fresh volume
```

---

## Summary

For Windows + MergeTree:
1. ✅ Use **named volume** (best solution)
2. ⚠️ Or run as **root** (quick fix, less secure)
3. 🔧 Or **chmod manually** (temporary)
4. 🐧 Or use **WSL2** (best dev experience)

Named volume is the **professional solution** - no permission issues, better performance, proper persistence.
