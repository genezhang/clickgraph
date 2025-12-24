# Docker Deployment Guide

This guide covers deploying ClickGraph using Docker in production environments.

## Quick Start

### Using Pre-built Image (Recommended)

Pull and run the latest image from Docker Hub:

```bash
docker pull genezhang/clickgraph:latest

docker run -d \
  --name clickgraph \
  -p 8080:8080 \
  -p 7687:7687 \
  -e CLICKHOUSE_URL="http://clickhouse:8123" \
  -e CLICKHOUSE_USER="default" \
  -e CLICKHOUSE_PASSWORD="password" \
  -e CLICKHOUSE_DATABASE="default" \
  genezhang/clickgraph:latest
```

### Using Docker Compose

The easiest way to run ClickGraph with ClickHouse:

```bash
# Clone repository
git clone https://github.com/genezhang/clickgraph
cd clickgraph

# Start services
docker-compose up -d

# Check status
docker-compose ps

# View logs
docker-compose logs -f clickgraph
```

## Configuration

### Environment Variables

#### ClickGraph Server Configuration

| Variable | Default | Description |
|----------|---------|-------------|
| `CLICKGRAPH_HOST` | `0.0.0.0` | HTTP server bind address |
| `CLICKGRAPH_PORT` | `8080` | HTTP server port |
| `CLICKGRAPH_BOLT_HOST` | `0.0.0.0` | Bolt protocol bind address |
| `CLICKGRAPH_BOLT_PORT` | `7687` | Bolt protocol port |
| `CLICKGRAPH_BOLT_ENABLED` | `true` | Enable/disable Bolt protocol |
| `CLICKGRAPH_MAX_CTE_DEPTH` | `100` | Max recursion depth for variable-length paths |
| `RUST_LOG` | `info` | Logging level (error/warn/info/debug/trace) |

#### ClickHouse Connection (Required)

| Variable | Required | Description |
|----------|----------|-------------|
| `CLICKHOUSE_URL` | ✅ Yes | ClickHouse HTTP endpoint (e.g., `http://clickhouse:8123`) |
| `CLICKHOUSE_USER` | ✅ Yes | ClickHouse username |
| `CLICKHOUSE_PASSWORD` | ✅ Yes | ClickHouse password |
| `CLICKHOUSE_DATABASE` | ⚪ Optional | Default database name (defaults to "default"). All queries use fully-qualified table names from schema config. |

#### Graph Schema Configuration

| Variable | Default | Description |
|----------|---------|-------------|
| `GRAPH_CONFIG_PATH` | None | Path to YAML schema config file |

**Note**: If `GRAPH_CONFIG_PATH` is not set, schema must be loaded via API (`POST /schemas/load`)

#### Query Cache Configuration

| Variable | Default | Description |
|----------|---------|-------------|
| `CLICKGRAPH_QUERY_CACHE_ENABLED` | `true` | Enable query plan caching |
| `CLICKGRAPH_QUERY_CACHE_MAX_ENTRIES` | `1000` | Maximum cache entries |
| `CLICKGRAPH_QUERY_CACHE_MAX_SIZE_MB` | `100` | Maximum cache size in MB |

### Command-Line Arguments

Override environment variables with CLI flags:

```bash
docker run genezhang/clickgraph:latest \
  --http-port 8081 \
  --bolt-port 7688 \
  --max-cte-depth 200 \
  --validate-schema
```

Available flags:
- `--http-host <HOST>` - HTTP server host
- `--http-port <PORT>` - HTTP server port
- `--bolt-host <HOST>` - Bolt server host
- `--bolt-port <PORT>` - Bolt server port
- `--disable-bolt` - Disable Bolt protocol
- `--max-cte-depth <DEPTH>` - Max CTE recursion depth (1-1000)
- `--validate-schema` - Validate schema against ClickHouse on startup

## Production Deployment

### Docker Compose Production Example

```yaml
version: '3.8'

services:
  clickhouse:
    image: clickhouse/clickhouse-server:25.8.11
    container_name: clickhouse
    environment:
      CLICKHOUSE_DB: "production"
      CLICKHOUSE_USER: "clickgraph_user"
      CLICKHOUSE_PASSWORD: "${CH_PASSWORD}" # Use secrets!
      CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT: "1"
    ports:
      - "8123:8123"
    volumes:
      - clickhouse_data:/var/lib/clickhouse
    healthcheck:
      test: ["CMD", "clickhouse-client", "--query", "SELECT 1"]
      interval: 10s
      timeout: 5s
      retries: 5
    restart: unless-stopped

  clickgraph:
    image: genezhang/clickgraph:0.5.0  # Pin to specific version
    container_name: clickgraph
    depends_on:
      clickhouse:
        condition: service_healthy
    environment:
      # ClickHouse connection
      CLICKHOUSE_URL: "http://clickhouse:8123"
      CLICKHOUSE_USER: "clickgraph_user"
      CLICKHOUSE_PASSWORD: "${CH_PASSWORD}" # Use secrets!
      CLICKHOUSE_DATABASE: "production"
      
      # Server config
      CLICKGRAPH_PORT: "8080"
      CLICKGRAPH_BOLT_PORT: "7687"
      CLICKGRAPH_BOLT_ENABLED: "true"
      CLICKGRAPH_MAX_CTE_DEPTH: "200"
      
      # Logging
      RUST_LOG: "warn" # Less verbose in production
      
      # Cache config
      CLICKGRAPH_QUERY_CACHE_ENABLED: "true"
      CLICKGRAPH_QUERY_CACHE_MAX_ENTRIES: "5000"
      CLICKGRAPH_QUERY_CACHE_MAX_SIZE_MB: "500"
      
      # Schema
      GRAPH_CONFIG_PATH: "/app/config/schema.yaml"
    ports:
      - "8080:8080"
      - "7687:7687"
    volumes:
      - ./config/schema.yaml:/app/config/schema.yaml:ro
      - ./schemas:/app/schemas:ro
    healthcheck:
      test: ["CMD", "wget", "--spider", "-q", "http://localhost:8080/health"]
      interval: 30s
      timeout: 3s
      retries: 3
      start_period: 10s
    restart: unless-stopped
    # Resource limits
    deploy:
      resources:
        limits:
          cpus: '2'
          memory: 2G
        reservations:
          cpus: '1'
          memory: 1G

volumes:
  clickhouse_data:
```

### Kubernetes Deployment

Example deployment manifest:

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: clickgraph
  labels:
    app: clickgraph
spec:
  replicas: 3
  selector:
    matchLabels:
      app: clickgraph
  template:
    metadata:
      labels:
        app: clickgraph
    spec:
      containers:
      - name: clickgraph
        image: genezhang/clickgraph:0.5.0
        ports:
        - containerPort: 8080
          name: http
        - containerPort: 7687
          name: bolt
        env:
        - name: CLICKHOUSE_URL
          value: "http://clickhouse-service:8123"
        - name: CLICKHOUSE_USER
          valueFrom:
            secretKeyRef:
              name: clickhouse-creds
              key: username
        - name: CLICKHOUSE_PASSWORD
          valueFrom:
            secretKeyRef:
              name: clickhouse-creds
              key: password
        - name: CLICKHOUSE_DATABASE
          value: "production"
        - name: CLICKGRAPH_MAX_CTE_DEPTH
          value: "200"
        - name: RUST_LOG
          value: "info"
        livenessProbe:
          httpGet:
            path: /health
            port: 8080
          initialDelaySeconds: 10
          periodSeconds: 30
        readinessProbe:
          httpGet:
            path: /health
            port: 8080
          initialDelaySeconds: 5
          periodSeconds: 10
        resources:
          requests:
            memory: "512Mi"
            cpu: "500m"
          limits:
            memory: "2Gi"
            cpu: "2000m"
        volumeMounts:
        - name: schema-config
          mountPath: /app/config
          readOnly: true
      volumes:
      - name: schema-config
        configMap:
          name: clickgraph-schema
---
apiVersion: v1
kind: Service
metadata:
  name: clickgraph
spec:
  selector:
    app: clickgraph
  ports:
  - name: http
    port: 8080
    targetPort: 8080
  - name: bolt
    port: 7687
    targetPort: 7687
  type: LoadBalancer
```

## Security Best Practices

### 1. Use Non-Root User

The Docker image runs as non-root user `clickgraph` (UID 1000) by default.

### 2. Read-Only Root Filesystem

Add read-only root filesystem for extra security:

```yaml
services:
  clickgraph:
    image: genezhang/clickgraph:latest
    read_only: true
    tmpfs:
      - /tmp
```

### 3. Use Secrets for Credentials

**Docker Compose with secrets:**

```yaml
services:
  clickgraph:
    image: genezhang/clickgraph:latest
    secrets:
      - clickhouse_password
    environment:
      CLICKHOUSE_PASSWORD_FILE: /run/secrets/clickhouse_password

secrets:
  clickhouse_password:
    file: ./secrets/clickhouse_password.txt
```

**Note**: ClickGraph currently reads from environment variables. Use external secret management (e.g., HashiCorp Vault, AWS Secrets Manager) and inject as env vars.

### 4. Network Isolation

Create dedicated networks:

```yaml
networks:
  backend:
    internal: true  # No external access
  frontend:
    # External access allowed

services:
  clickhouse:
    networks:
      - backend
  
  clickgraph:
    networks:
      - frontend
      - backend
```

### 5. Resource Limits

Always set resource limits in production:

```yaml
services:
  clickgraph:
    deploy:
      resources:
        limits:
          cpus: '2'
          memory: 2G
```

## Monitoring

### Health Checks

**HTTP endpoint:**
```bash
curl http://localhost:8080/health
# Returns: {"status":"healthy"}
```

**Docker health status:**
```bash
docker ps --format "table {{.Names}}\t{{.Status}}"
```

### Logs

**View logs:**
```bash
# Docker Compose
docker-compose logs -f clickgraph

# Docker
docker logs -f clickgraph

# Last 100 lines
docker logs --tail 100 clickgraph
```

**Log levels:**
Set via `RUST_LOG` environment variable:
- `error` - Errors only
- `warn` - Warnings and errors
- `info` - Default, general information (recommended for production)
- `debug` - Detailed debugging information
- `trace` - Very verbose, all details

**Structured logging:**
```bash
RUST_LOG=clickgraph=debug,tower_http=debug
```

### Metrics

ClickGraph includes performance metrics in HTTP response headers:

```bash
curl -v http://localhost:8080/query
# Response headers:
# X-Query-Cache-Status: HIT|MISS|BYPASS
# X-Parse-Time-Ms: 5
# X-Plan-Time-Ms: 12
# X-SQL-Gen-Time-Ms: 3
# X-CH-Exec-Time-Ms: 145
# X-Total-Time-Ms: 165
```

## Troubleshooting

### Container Won't Start

**Check logs:**
```bash
docker logs clickgraph
```

**Common issues:**
1. **Port conflict** - Another service using 8080 or 7687
   ```bash
   # Check what's using the port
   netstat -tulpn | grep 8080
   # Use different port
   docker run -p 8081:8080 genezhang/clickgraph:latest --http-port 8080
   ```

2. **Missing ClickHouse connection** - Check environment variables
   ```bash
   docker exec clickgraph env | grep CLICKHOUSE
   ```

3. **Permission issues** - Volume mount permissions
   ```bash
   # Ensure files are readable by UID 1000
   chown -R 1000:1000 ./schemas
   ```

### Connection Refused

**Test ClickHouse connectivity from container:**
```bash
docker exec clickgraph wget -O- http://clickhouse:8123/ping
```

**Check network connectivity:**
```bash
docker network inspect clickgraph_default
```

### Performance Issues

**Check resource usage:**
```bash
docker stats clickgraph
```

**Increase cache size:**
```yaml
environment:
  CLICKGRAPH_QUERY_CACHE_MAX_ENTRIES: "10000"
  CLICKGRAPH_QUERY_CACHE_MAX_SIZE_MB: "1000"
```

**Increase CTE depth for complex queries:**
```yaml
environment:
  CLICKGRAPH_MAX_CTE_DEPTH: "500"
```

## Building Custom Images

### Build Locally

```bash
# Build with specific tag
docker build -t my-clickgraph:custom .

# Multi-platform build
docker buildx build --platform linux/amd64,linux/arm64 -t my-clickgraph:custom .
```

### Customize Dockerfile

Extend the official image:

```dockerfile
FROM genezhang/clickgraph:0.5.0

# Add custom schema
COPY my-schema.yaml /app/config/schema.yaml
ENV GRAPH_CONFIG_PATH=/app/config/schema.yaml

# Add custom startup script
COPY startup.sh /app/startup.sh
ENTRYPOINT ["/app/startup.sh"]
```

## Upgrading

### Version Pinning (Recommended)

Always pin to specific versions in production:

```yaml
services:
  clickgraph:
    image: genezhang/clickgraph:0.5.0  # Specific version
    # NOT: genezhang/clickgraph:latest
```

### Upgrade Process

```bash
# 1. Pull new version
docker pull genezhang/clickgraph:0.5.1

# 2. Update docker-compose.yaml
# 3. Recreate container
docker-compose up -d clickgraph

# 4. Check health
docker-compose ps
docker-compose logs -f clickgraph
```

### Rollback

```bash
# Rollback to previous version
docker-compose down
# Edit docker-compose.yaml to use old version
docker-compose up -d
```

## Support

- **Documentation**: https://github.com/genezhang/clickgraph/tree/main/docs
- **Issues**: https://github.com/genezhang/clickgraph/issues
- **Docker Hub**: https://hub.docker.com/r/genezhang/clickgraph
