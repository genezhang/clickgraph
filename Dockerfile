# ============================================================================
# Stage 1: Planner - Generate dependency recipe for caching
# ============================================================================
FROM lukemathwalker/cargo-chef:latest-rust-1-bullseye AS chef
WORKDIR /app

FROM chef AS planner
COPY . .
RUN cargo chef prepare --recipe-path recipe.json

# ============================================================================
# Stage 2: Builder - Build the application
# ============================================================================
FROM chef AS builder 

# Copy dependency recipe
COPY --from=planner /app/recipe.json recipe.json

# Build dependencies - this is the caching Docker layer!
# This layer is only rebuilt when dependencies change
RUN cargo chef cook --release --recipe-path recipe.json

# Copy source code
COPY . .

# Build the application binaries
RUN cargo build --release --bin clickgraph && \
    cargo build --release -p clickgraph-client --bin clickgraph-client

# Strip debug symbols to reduce binary size
RUN strip /app/target/release/clickgraph && \
    strip /app/target/release/clickgraph-client

# ============================================================================
# Stage 3: Runtime - Minimal production image
# ============================================================================
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

# Copy binaries from builder
COPY --from=builder /app/target/release/clickgraph /usr/local/bin/clickgraph
COPY --from=builder /app/target/release/clickgraph-client /usr/local/bin/clickgraph-client

# Set proper permissions
RUN chown -R clickgraph:clickgraph /app && \
    chmod +x /usr/local/bin/clickgraph /usr/local/bin/clickgraph-client

# Switch to non-root user
USER clickgraph

# Expose ports
EXPOSE 8080 7687

# Health check using wget (smaller than curl)
HEALTHCHECK --interval=30s --timeout=3s --start-period=10s --retries=3 \
    CMD wget --no-verbose --tries=1 --spider http://localhost:8080/health || exit 1

# Default environment variables (can be overridden)
ENV CLICKGRAPH_HOST=0.0.0.0 \
    CLICKGRAPH_PORT=8080 \
    CLICKGRAPH_BOLT_HOST=0.0.0.0 \
    CLICKGRAPH_BOLT_PORT=7687 \
    CLICKGRAPH_BOLT_ENABLED=true \
    CLICKGRAPH_MAX_CTE_DEPTH=100 \
    RUST_LOG=info

# Set entrypoint
ENTRYPOINT ["/usr/local/bin/clickgraph"]

# Default command (can be overridden)
CMD []