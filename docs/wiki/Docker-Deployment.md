# Docker Deployment Guide

Complete guide for deploying ClickGraph in production using Docker and Docker Compose.

## Table of Contents
- [Quick Production Setup](#quick-production-setup)
- [Docker Compose Configuration](#docker-compose-configuration)
- [Environment Configuration](#environment-configuration)
- [Networking and Ports](#networking-and-ports)
- [Volume Management](#volume-management)
- [Security Configuration](#security-configuration)
- [Multi-Container Architecture](#multi-container-architecture)
- [Health Checks and Monitoring](#health-checks-and-monitoring)
- [Scaling and Load Balancing](#scaling-and-load-balancing)
- [Troubleshooting](#troubleshooting)

---

## Quick Production Setup

Get ClickGraph running in production with Docker in 10 minutes.

### Prerequisites

- Docker 24.0+ and Docker Compose 2.0+
- 4GB RAM minimum (8GB recommended)
- Production ClickHouse instance or cluster
- SSL certificates (for HTTPS/TLS)

### Basic Production Deployment

```bash
# 1. Clone and prepare
git clone https://github.com/genezhang/clickgraph.git
cd clickgraph

# 2. Create production environment file
cat > .env.production <<EOF
CLICKHOUSE_URL=http://clickhouse:8123
CLICKHOUSE_USER=clickgraph_user
CLICKHOUSE_PASSWORD=your_secure_password
CLICKHOUSE_DATABASE=production_graph
GRAPH_CONFIG_PATH=/app/schemas/production.yaml
RUST_LOG=info
EOF

# 3. Start production stack
docker-compose -f docker-compose.prod.yaml up -d

# 4. Verify deployment
curl http://localhost:8080/health
```

---

## Docker Compose Configuration

### Development Configuration

**File**: `docker-compose.yaml` (included in repo)

```yaml
version: '3.8'

services:
  clickhouse:
    image: clickhouse/clickhouse-server:latest
    container_name: clickhouse-clickhouse
    ports:
      - "8123:8123"  # HTTP interface
      - "9000:9000"  # Native protocol
    environment:
      CLICKHOUSE_DB: brahmand
      CLICKHOUSE_USER: test_user
      CLICKHOUSE_PASSWORD: test_pass
    volumes:
      - clickhouse_data:/var/lib/clickhouse
    healthcheck:
      test: ["CMD", "clickhouse-client", "--query", "SELECT 1"]
      interval: 10s
      timeout: 5s
      retries: 5

  clickgraph:
    build: .
    container_name: clickgraph
    ports:
      - "8080:8080"  # HTTP API
      - "7687:7687"  # Bolt protocol
    environment:
      CLICKHOUSE_URL: http://clickhouse:8123
      CLICKHOUSE_USER: test_user
      CLICKHOUSE_PASSWORD: test_pass
      CLICKHOUSE_DATABASE: brahmand
      GRAPH_CONFIG_PATH: /app/benchmarks/social_network/schemas/social_benchmark.yaml
      RUST_LOG: info
    depends_on:
      clickhouse:
        condition: service_healthy
    volumes:
      - ./schemas:/app/schemas:ro
    restart: unless-stopped

volumes:
  clickhouse_data:
```

### Production Configuration

**File**: `docker-compose.prod.yaml` (create this)

```yaml
version: '3.8'

services:
  clickgraph:
    image: clickgraph:latest
    container_name: clickgraph-prod
    ports:
      - "8080:8080"  # HTTP API
      - "7687:7687"  # Bolt protocol
    environment:
      # ClickHouse connection
      CLICKHOUSE_URL: ${CLICKHOUSE_URL}
      CLICKHOUSE_USER: ${CLICKHOUSE_USER}
      CLICKHOUSE_PASSWORD: ${CLICKHOUSE_PASSWORD}
      CLICKHOUSE_DATABASE: ${CLICKHOUSE_DATABASE}
      
      # Schema configuration
      GRAPH_CONFIG_PATH: ${GRAPH_CONFIG_PATH}
      
      # Server configuration
      RUST_LOG: ${RUST_LOG:-info}
      
      # Resource limits
      MAX_RECURSION_DEPTH: ${MAX_RECURSION_DEPTH:-100}
    
    # Security
    read_only: true
    security_opt:
      - no-new-privileges:true
    cap_drop:
      - ALL
    
    # Resource limits
    deploy:
      resources:
        limits:
          cpus: '2'
          memory: 2G
        reservations:
          cpus: '0.5'
          memory: 512M
    
    # Health check
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:8080/health"]
      interval: 30s
      timeout: 10s
      retries: 3
      start_period: 40s
    
    # Volumes (read-only)
    volumes:
      - ./schemas:/app/schemas:ro
      - /tmp:/tmp  # For read_only filesystem
    
    # Restart policy
    restart: always
    
    # Logging
    logging:
      driver: "json-file"
      options:
        max-size: "10m"
        max-file: "3"
    
    # Network
    networks:
      - clickgraph-network

networks:
  clickgraph-network:
    driver: bridge
```

### Build Production Image

**Dockerfile.prod** (optimized for production):

```dockerfile
# Multi-stage build for minimal image size
FROM rust:1.85-slim AS builder

WORKDIR /build

# Install dependencies
RUN apt-get update && apt-get install -y \
    pkg-config \
    libssl-dev \
    && rm -rf /var/lib/apt/lists/*

# Copy manifests
COPY Cargo.toml Cargo.lock ./
COPY brahmand/Cargo.toml ./brahmand/

# Build dependencies only (cache layer)
RUN mkdir src && echo "fn main() {}" > src/main.rs && \
    cargo build --release && \
    rm -rf src

# Copy source code
COPY . .

# Build application
RUN cargo build --release --bin clickgraph

# Runtime stage
FROM debian:bookworm-slim

# Install runtime dependencies
RUN apt-get update && apt-get install -y \
    ca-certificates \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Create non-root user
RUN useradd -r -u 1000 -m -s /bin/bash clickgraph

WORKDIR /app

# Copy binary from builder
COPY --from=builder /build/target/release/clickgraph /app/

# Copy schemas
COPY --chown=clickgraph:clickgraph schemas /app/schemas

# Switch to non-root user
USER clickgraph

# Expose ports
EXPOSE 8080 7687

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=40s --retries=3 \
  CMD curl -f http://localhost:8080/health || exit 1

# Run application
CMD ["/app/clickgraph"]
```

Build and tag:

```bash
# Build production image
docker build -f Dockerfile.prod -t clickgraph:latest -t clickgraph:v0.4.0 .

# Push to registry
docker tag clickgraph:latest your-registry.com/clickgraph:latest
docker push your-registry.com/clickgraph:latest
```

---

## Environment Configuration

### Production Environment Variables

**File**: `.env.production`

```bash
# ClickHouse Connection (Required)
CLICKHOUSE_URL=http://clickhouse-prod.internal:8123
CLICKHOUSE_USER=clickgraph_user
CLICKHOUSE_PASSWORD=your_secure_password_here
CLICKHOUSE_DATABASE=production_graph

# Schema Configuration (Required)
GRAPH_CONFIG_PATH=/app/schemas/production/main.yaml

# Server Configuration
RUST_LOG=info,clickgraph=debug
MAX_RECURSION_DEPTH=100

# Optional: Multi-Tenancy
# (Passed per-request via API, not set globally)

# Optional: RBAC
# (Passed per-request via API, not set globally)
```

### Secure Secrets Management

**Using Docker Secrets** (Docker Swarm/Kubernetes):

```yaml
# docker-compose.secrets.yaml
version: '3.8'

services:
  clickgraph:
    image: clickgraph:latest
    secrets:
      - clickhouse_password
      - graph_schema
    environment:
      CLICKHOUSE_URL: http://clickhouse:8123
      CLICKHOUSE_USER: clickgraph_user
      CLICKHOUSE_PASSWORD_FILE: /run/secrets/clickhouse_password
      CLICKHOUSE_DATABASE: production_graph
      GRAPH_CONFIG_PATH: /run/secrets/graph_schema

secrets:
  clickhouse_password:
    external: true
  graph_schema:
    file: ./schemas/production/main.yaml
```

Create secrets:

```bash
# Create password secret
echo "your_secure_password" | docker secret create clickhouse_password -

# Create schema secret
docker secret create graph_schema ./schemas/production/main.yaml

# Deploy with secrets
docker stack deploy -c docker-compose.secrets.yaml clickgraph
```

**Using Environment File** (simpler for Docker Compose):

```bash
# Load from .env file
docker-compose --env-file .env.production up -d

# Or export variables
export $(cat .env.production | xargs)
docker-compose up -d
```

---

## Networking and Ports

### Port Configuration

| Port | Protocol | Purpose | Expose to |
|------|----------|---------|-----------|
| 8080 | HTTP | REST API | Load balancer / Reverse proxy |
| 7687 | Bolt | Neo4j protocol | Load balancer / Reverse proxy |
| 8123 | HTTP | ClickHouse interface | Internal only |
| 9000 | TCP | ClickHouse native | Internal only |

### Custom Ports

```yaml
services:
  clickgraph:
    ports:
      - "8081:8080"  # Map to different host port
      - "7688:7687"
    command: ["--http-port", "8080", "--bolt-port", "7687"]
```

Or via environment:

```bash
# Note: These change the ports inside the container
# Typically you'd change the host mapping instead
docker run -p 8081:8080 -p 7688:7687 clickgraph:latest
```

### Reverse Proxy Configuration

**Nginx** (recommended for production):

```nginx
# /etc/nginx/sites-available/clickgraph
upstream clickgraph_http {
    least_conn;
    server clickgraph-1:8080 max_fails=3 fail_timeout=30s;
    server clickgraph-2:8080 max_fails=3 fail_timeout=30s;
    keepalive 32;
}

upstream clickgraph_bolt {
    least_conn;
    server clickgraph-1:7687;
    server clickgraph-2:7687;
}

# HTTP API
server {
    listen 80;
    server_name api.clickgraph.example.com;
    
    # Redirect to HTTPS
    return 301 https://$server_name$request_uri;
}

server {
    listen 443 ssl http2;
    server_name api.clickgraph.example.com;
    
    # SSL configuration
    ssl_certificate /etc/ssl/certs/clickgraph.crt;
    ssl_certificate_key /etc/ssl/private/clickgraph.key;
    ssl_protocols TLSv1.2 TLSv1.3;
    ssl_ciphers HIGH:!aNULL:!MD5;
    
    # Security headers
    add_header Strict-Transport-Security "max-age=31536000" always;
    add_header X-Frame-Options "SAMEORIGIN" always;
    add_header X-Content-Type-Options "nosniff" always;
    
    location / {
        proxy_pass http://clickgraph_http;
        proxy_http_version 1.1;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
        
        # Timeouts
        proxy_connect_timeout 60s;
        proxy_send_timeout 60s;
        proxy_read_timeout 60s;
        
        # Connection reuse
        proxy_set_header Connection "";
    }
    
    location /health {
        proxy_pass http://clickgraph_http/health;
        access_log off;
    }
}

# Bolt Protocol (TCP stream)
stream {
    upstream clickgraph_bolt_stream {
        least_conn;
        server clickgraph-1:7687;
        server clickgraph-2:7687;
    }
    
    server {
        listen 7687;
        proxy_pass clickgraph_bolt_stream;
        proxy_connect_timeout 10s;
    }
}
```

**Traefik** (Docker-native):

```yaml
# docker-compose.traefik.yaml
version: '3.8'

services:
  traefik:
    image: traefik:v2.10
    command:
      - "--api.insecure=true"
      - "--providers.docker=true"
      - "--entrypoints.web.address=:80"
      - "--entrypoints.websecure.address=:443"
      - "--entrypoints.bolt.address=:7687"
    ports:
      - "80:80"
      - "443:443"
      - "7687:7687"
    volumes:
      - /var/run/docker.sock:/var/run/docker.sock:ro
      - ./traefik/certs:/certs:ro

  clickgraph:
    image: clickgraph:latest
    labels:
      # HTTP routing
      - "traefik.enable=true"
      - "traefik.http.routers.clickgraph.rule=Host(`api.clickgraph.example.com`)"
      - "traefik.http.routers.clickgraph.entrypoints=websecure"
      - "traefik.http.routers.clickgraph.tls=true"
      - "traefik.http.services.clickgraph.loadbalancer.server.port=8080"
      
      # Bolt routing
      - "traefik.tcp.routers.clickgraph-bolt.rule=HostSNI(`*`)"
      - "traefik.tcp.routers.clickgraph-bolt.entrypoints=bolt"
      - "traefik.tcp.services.clickgraph-bolt.loadbalancer.server.port=7687"
```

### Network Isolation

```yaml
# docker-compose.network.yaml
version: '3.8'

networks:
  # Public network (exposed to internet via reverse proxy)
  public:
    driver: bridge
  
  # Internal network (backend services)
  internal:
    driver: bridge
    internal: true

services:
  clickgraph:
    networks:
      - public   # For client connections
      - internal # For ClickHouse connection
  
  clickhouse:
    networks:
      - internal # Not exposed to public
```

---

## Volume Management

### Schema Volumes

```yaml
volumes:
  # Mount schema as read-only
  - ./schemas:/app/schemas:ro
  
  # Or use named volume
  - schema_data:/app/schemas:ro

volumes:
  schema_data:
    driver: local
    driver_opts:
      type: none
      o: bind
      device: /path/to/schemas
```

### ClickHouse Data Persistence

```yaml
services:
  clickhouse:
    volumes:
      # Data directory
      - clickhouse_data:/var/lib/clickhouse
      
      # Configuration
      - ./clickhouse/config.xml:/etc/clickhouse-server/config.xml:ro
      
      # Users configuration
      - ./clickhouse/users.xml:/etc/clickhouse-server/users.xml:ro
      
      # Logs
      - clickhouse_logs:/var/log/clickhouse-server

volumes:
  clickhouse_data:
    driver: local
  clickhouse_logs:
    driver: local
```

### Backup Strategies

```bash
# Backup ClickHouse data
docker run --rm \
  -v clickhouse_data:/data \
  -v $(pwd)/backups:/backup \
  alpine tar czf /backup/clickhouse-$(date +%Y%m%d).tar.gz /data

# Restore ClickHouse data
docker run --rm \
  -v clickhouse_data:/data \
  -v $(pwd)/backups:/backup \
  alpine tar xzf /backup/clickhouse-20251117.tar.gz -C /

# Schema backup (simple file copy)
cp -r schemas backups/schemas-$(date +%Y%m%d)
```

---

## Security Configuration

### Container Security

```yaml
services:
  clickgraph:
    # Run as non-root user
    user: "1000:1000"
    
    # Read-only filesystem
    read_only: true
    
    # Drop all capabilities
    cap_drop:
      - ALL
    
    # No new privileges
    security_opt:
      - no-new-privileges:true
    
    # Temporary directories
    tmpfs:
      - /tmp:size=100M,mode=1777
```

### TLS/SSL Configuration

**For ClickHouse Connection**:

```yaml
environment:
  CLICKHOUSE_URL: https://clickhouse:8443
  CLICKHOUSE_SSL_VERIFY: "true"
  CLICKHOUSE_SSL_CERT: /certs/client.crt
  CLICKHOUSE_SSL_KEY: /certs/client.key
  CLICKHOUSE_SSL_CA: /certs/ca.crt

volumes:
  - ./certs:/certs:ro
```

### Secrets Management

```bash
# Use Docker secrets (Swarm mode)
echo "secure_password" | docker secret create ch_password -

# Or use HashiCorp Vault
docker run -e VAULT_ADDR=https://vault:8200 \
  -e VAULT_TOKEN=s.xxx \
  clickgraph:latest
```

### Resource Limits

```yaml
deploy:
  resources:
    limits:
      cpus: '4'      # Max 4 CPUs
      memory: 4G     # Max 4GB RAM
    reservations:
      cpus: '1'      # Guaranteed 1 CPU
      memory: 1G     # Guaranteed 1GB RAM
```

---

## Multi-Container Architecture

### Three-Tier Architecture

```yaml
version: '3.8'

services:
  # Load Balancer
  nginx:
    image: nginx:alpine
    ports:
      - "80:80"
      - "443:443"
    volumes:
      - ./nginx.conf:/etc/nginx/nginx.conf:ro
      - ./certs:/etc/ssl:ro
    depends_on:
      - clickgraph-1
      - clickgraph-2
    networks:
      - frontend

  # ClickGraph instances (scalable)
  clickgraph-1:
    image: clickgraph:latest
    environment:
      <<: *common-env
    networks:
      - frontend
      - backend
    deploy:
      replicas: 2

  clickgraph-2:
    image: clickgraph:latest
    environment:
      <<: *common-env
    networks:
      - frontend
      - backend

  # ClickHouse cluster
  clickhouse-1:
    image: clickhouse/clickhouse-server:latest
    volumes:
      - ch1_data:/var/lib/clickhouse
      - ./clickhouse/cluster.xml:/etc/clickhouse-server/config.d/cluster.xml:ro
    networks:
      - backend

  clickhouse-2:
    image: clickhouse/clickhouse-server:latest
    volumes:
      - ch2_data:/var/lib/clickhouse
      - ./clickhouse/cluster.xml:/etc/clickhouse-server/config.d/cluster.xml:ro
    networks:
      - backend

networks:
  frontend:
    driver: bridge
  backend:
    driver: bridge
    internal: true

volumes:
  ch1_data:
  ch2_data:

# Common environment variables
x-common-env: &common-env
  CLICKHOUSE_URL: http://clickhouse-1:8123,http://clickhouse-2:8123
  CLICKHOUSE_USER: clickgraph_user
  CLICKHOUSE_PASSWORD: ${CLICKHOUSE_PASSWORD}
  CLICKHOUSE_DATABASE: production_graph
  GRAPH_CONFIG_PATH: /app/schemas/production.yaml
```

---

## Health Checks and Monitoring

### Health Check Configuration

```yaml
healthcheck:
  test: ["CMD", "curl", "-f", "http://localhost:8080/health"]
  interval: 30s      # Check every 30 seconds
  timeout: 10s       # Fail if takes > 10s
  retries: 3         # Try 3 times before marking unhealthy
  start_period: 40s  # Grace period during startup
```

### Monitoring Endpoints

```bash
# Health check
curl http://localhost:8080/health
# {"status":"ok","clickhouse":"connected","version":"0.4.0"}

# Schema info
curl http://localhost:8080/schema
# Returns loaded schema details

# Metrics (if enabled)
curl http://localhost:8080/metrics
# Prometheus-style metrics
```

### Logging Configuration

```yaml
logging:
  driver: "json-file"
  options:
    max-size: "10m"     # Max 10MB per log file
    max-file: "3"       # Keep 3 files
    compress: "true"    # Compress rotated logs

# Or use syslog
logging:
  driver: "syslog"
  options:
    syslog-address: "tcp://logserver:514"
    tag: "clickgraph"

# Or forward to ELK/Loki
logging:
  driver: "fluentd"
  options:
    fluentd-address: "localhost:24224"
    tag: "docker.clickgraph"
```

View logs:

```bash
# Follow logs
docker-compose logs -f clickgraph

# Last 100 lines
docker-compose logs --tail=100 clickgraph

# Specific time range
docker-compose logs --since 2025-11-17T10:00:00 clickgraph
```

---

## Scaling and Load Balancing

### Horizontal Scaling

```bash
# Scale to 3 instances
docker-compose up -d --scale clickgraph=3

# Check running instances
docker-compose ps
```

### Docker Swarm Deployment

```bash
# Initialize swarm
docker swarm init

# Deploy stack
docker stack deploy -c docker-compose.swarm.yaml clickgraph

# Scale service
docker service scale clickgraph_clickgraph=5

# Check service status
docker service ps clickgraph_clickgraph
```

**docker-compose.swarm.yaml**:

```yaml
version: '3.8'

services:
  clickgraph:
    image: clickgraph:latest
    deploy:
      replicas: 3
      update_config:
        parallelism: 1
        delay: 10s
        order: start-first
      rollback_config:
        parallelism: 1
        delay: 10s
      restart_policy:
        condition: on-failure
        delay: 5s
        max_attempts: 3
      placement:
        constraints:
          - node.role == worker
        preferences:
          - spread: node.labels.zone
```

### Load Balancing Strategies

**Round Robin** (default):
```yaml
# nginx.conf
upstream clickgraph {
    server clickgraph-1:8080;
    server clickgraph-2:8080;
    server clickgraph-3:8080;
}
```

**Least Connections** (recommended for graph queries):
```yaml
upstream clickgraph {
    least_conn;
    server clickgraph-1:8080;
    server clickgraph-2:8080;
}
```

**IP Hash** (session affinity):
```yaml
upstream clickgraph {
    ip_hash;
    server clickgraph-1:8080;
    server clickgraph-2:8080;
}
```

---

## Troubleshooting

### Container Won't Start

```bash
# Check logs
docker-compose logs clickgraph

# Common issues:
# 1. Can't connect to ClickHouse
#    Solution: Check CLICKHOUSE_URL, verify ClickHouse is running

# 2. Schema file not found
#    Solution: Check GRAPH_CONFIG_PATH and volume mount

# 3. Port already in use
#    Solution: Change port mapping or stop conflicting service
```

### Health Check Failing

```bash
# Manual health check
docker exec clickgraph curl http://localhost:8080/health

# If connection refused:
# - Check if server started (view logs)
# - Verify port configuration
# - Check firewall rules
```

### Performance Issues

```bash
# Check resource usage
docker stats clickgraph

# Check ClickHouse connection
docker exec clickgraph curl http://clickhouse:8123/ping

# Enable debug logging
docker-compose up -d -e RUST_LOG=debug
```

### Network Issues

```bash
# Test inter-container connectivity
docker exec clickgraph ping clickhouse

# Check network configuration
docker network inspect clickgraph_default

# Test DNS resolution
docker exec clickgraph nslookup clickhouse
```

---

## Next Steps

Now that you have ClickGraph deployed with Docker:

- **[Production Best Practices](Production-Best-Practices.md)** - Security, performance, monitoring
- **[Performance Tuning](Performance-Query-Optimization.md)** - Optimize query performance
- **[Kubernetes Deployment](Kubernetes-Deployment.md)** - Deploy on K8s clusters
- **[Monitoring Guide](Monitoring-Observability.md)** - Set up comprehensive monitoring

---

[← Back to Home](Home.md) | [Next: Production Best Practices →](Production-Best-Practices.md)
