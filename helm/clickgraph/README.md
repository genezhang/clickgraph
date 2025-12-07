# ClickGraph Helm Chart

⚠️ **WARNING**: This Helm chart has NOT been tested yet. It requires validation before production use.

See [TESTING_CHECKLIST.md](./TESTING_CHECKLIST.md) for required testing procedures.

## Overview

This Helm chart deploys ClickGraph, a stateless graph query engine for ClickHouse®, on Kubernetes.

## Prerequisites

- Kubernetes 1.20+
- Helm 3.8+
- ClickHouse instance (external or use bundled option)
- Docker image: `clickgraph/clickgraph:0.5.0`

## Quick Start

⚠️ **Test in non-production environment first!**

```bash
# Add repository (when published)
helm repo add clickgraph https://charts.clickgraph.io
helm repo update

# Install with default values
helm install my-clickgraph clickgraph/clickgraph \
  --namespace clickgraph \
  --create-namespace \
  --set clickhouse.external.existingSecret=my-clickhouse-secret
```

## Configuration

See [values.yaml](./values.yaml) for all configuration options.

### Key Configuration Options

| Parameter | Description | Default |
|-----------|-------------|---------|
| `replicaCount` | Number of replicas | `3` |
| `image.repository` | Docker image repository | `clickgraph/clickgraph` |
| `image.tag` | Docker image tag | `""` (uses Chart appVersion) |
| `resources.requests.cpu` | CPU request | `1000m` |
| `resources.requests.memory` | Memory request | `2Gi` |
| `resources.limits.cpu` | CPU limit | `4000m` |
| `resources.limits.memory` | Memory limit | `4Gi` |
| `autoscaling.enabled` | Enable HPA | `false` |
| `autoscaling.minReplicas` | Minimum replicas | `3` |
| `autoscaling.maxReplicas` | Maximum replicas | `10` |
| `clickhouse.external.enabled` | Use external ClickHouse | `true` |
| `clickhouse.external.host` | ClickHouse host | `clickhouse.default.svc.cluster.local` |
| `clickhouse.external.existingSecret` | Secret with ClickHouse password | (required) |

### Schema Configuration

Define graph schemas inline in `values.yaml`:

```yaml
schemas:
  create: true
  configs:
    social:
      name: social_graph
      version: "1.0"
      graph_schema:
        nodes:
          - label: User
            database: brahmand
            table: users_bench
            node_id: user_id
            property_mappings:
              user_id: user_id
              name: full_name
```

## Testing Status

**Current Status**: ⚠️ UNTESTED

Before using this chart, complete the testing checklist:

1. ✅ YAML syntax validation
2. ⏳ Helm lint
3. ⏳ Dry-run installation
4. ⏳ Template rendering
5. ⏳ Actual installation
6. ⏳ Pod health verification
7. ⏳ Query execution test
8. ⏳ Autoscaling verification
9. ⏳ High availability testing

See [TESTING_CHECKLIST.md](./TESTING_CHECKLIST.md) for detailed testing procedures.

## Known Limitations

Since this chart is untested, potential issues include:

- ConfigMap YAML indentation may be incorrect
- Environment variable formatting may need adjustment
- Volume mount paths may not match container expectations
- Resource limits may need tuning for actual workloads
- Probe configurations may need adjustment

## Documentation

For complete deployment documentation, see:
- [Kubernetes Deployment Guide](../../docs/wiki/Kubernetes-Deployment.md)
- [Production Best Practices](../../docs/wiki/Production-Best-Practices.md)

## Contributing

If you test this chart, please:

1. Document results in TESTING_CHECKLIST.md
2. Submit issues for any problems found
3. Open PRs with fixes
4. Update this README with findings

## License

Same as ClickGraph project license.
