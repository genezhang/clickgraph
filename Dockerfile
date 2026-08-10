# ============================================================================
# Dockerfile - Production build
# Schema discovery uses LLM via :discover command in clickgraph-client
# ============================================================================
# Stage 1: Planner - Generate dependency recipe for caching
FROM lukemathwalker/cargo-chef:latest-rust-1-bullseye AS chef
WORKDIR /app

FROM chef AS planner
COPY . .
RUN cargo chef prepare --recipe-path recipe.json

# ============================================================================
# Stage 2: Builder - Build the application
FROM chef AS builder 

# Copy dependency recipe
COPY --from=planner /app/recipe.json recipe.json

# Build dependencies - caching layer
RUN cargo chef cook --release --recipe-path recipe.json

# Copy source code
COPY . .

# Build the application binaries.
#
# `--features databricks` compiles in the DeltaGraph (Databricks SQL Warehouse)
# executor and Spark-SQL dialect, and lets us build the `deltagraph` server bin
# alongside `clickgraph` in a single pass. The feature is chdb-free (adds no
# heavy deps — `reqwest` is already non-optional), so the `clickgraph` binary is
# unchanged in behavior: its Databricks code is inert unless `--databricks` is
# passed. See docs/deltagraph/PACKAGING.md.
RUN cargo build --release --features databricks --bin clickgraph --bin deltagraph && \
    cargo build --release -p clickgraph-client --bin clickgraph-client

# Strip debug symbols to reduce binary size
RUN strip /app/target/release/clickgraph && \
    strip /app/target/release/deltagraph && \
    strip /app/target/release/clickgraph-client

# ============================================================================
# Stage 3: Runtime - Minimal production image
FROM debian:bullseye-slim AS runtime

# Install runtime dependencies only
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        ca-certificates \
        libssl1.1 \
        wget \
    && rm -rf /var/lib/apt/lists/*

# Create non-root user for security
RUN useradd -m -u 1000 -s /bin/bash clickgraph

# Set working directory
WORKDIR /app

# Copy binaries from builder.
# Both server binaries ship in one image: `clickgraph` (ClickHouse, the default
# entrypoint) and `deltagraph` (Databricks). Select DeltaGraph at runtime with
#   docker run --entrypoint /usr/local/bin/deltagraph ...
# See docs/deltagraph/DOCKER_QUICKSTART.md.
COPY --from=builder /app/target/release/clickgraph /usr/local/bin/clickgraph
COPY --from=builder /app/target/release/deltagraph /usr/local/bin/deltagraph
COPY --from=builder /app/target/release/clickgraph-client /usr/local/bin/clickgraph-client

# Set proper permissions
RUN chown -R clickgraph:clickgraph /app && \
    chmod +x /usr/local/bin/clickgraph /usr/local/bin/deltagraph /usr/local/bin/clickgraph-client

# Switch to non-root user
USER clickgraph

# Expose ports
EXPOSE 7475 7687

# Health check using wget (smaller than curl)
HEALTHCHECK --interval=30s --timeout=3s --start-period=10s --retries=3 \
    CMD wget --no-verbose --tries=1 --spider http://localhost:7475/health || exit 1

# Default environment variables (can be overridden)
ENV CLICKGRAPH_HOST=0.0.0.0 \
    CLICKGRAPH_PORT=7475 \
    CLICKGRAPH_BOLT_HOST=0.0.0.0 \
    CLICKGRAPH_BOLT_PORT=7687 \
    CLICKGRAPH_BOLT_ENABLED=true \
    CLICKGRAPH_MAX_CTE_DEPTH=100 \
    RUST_LOG=info

# Set entrypoint
ENTRYPOINT ["/usr/local/bin/clickgraph"]

# Default command (can be overridden)
CMD []
