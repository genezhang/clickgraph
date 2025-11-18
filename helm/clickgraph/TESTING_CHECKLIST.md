# Helm Chart Testing Checklist

⚠️ **WARNING**: This Helm chart has NOT been tested yet. It was generated based on Kubernetes best practices but requires validation before production use.

## Pre-Testing Requirements

- [ ] Helm 3.x installed
- [ ] kubectl configured with cluster access
- [ ] ClickHouse instance available (or use bundled option)
- [ ] Docker image built and available: `clickgraph/clickgraph:0.5.0`

## Basic Validation

### 1. Syntax Validation
```bash
# Lint the chart
helm lint ./helm/clickgraph

# Validate YAML syntax
helm template clickgraph ./helm/clickgraph --validate
```

### 2. Dry Run Installation
```bash
# Generate manifests without installing
helm install clickgraph ./helm/clickgraph \
  --dry-run --debug \
  --set clickhouse.external.existingSecret=test-secret
```

### 3. Template Rendering
```bash
# Check rendered templates
helm template clickgraph ./helm/clickgraph \
  --set clickhouse.external.existingSecret=test-secret \
  > /tmp/rendered.yaml

# Review the output
cat /tmp/rendered.yaml
```

## Installation Testing

### 4. Create Test Secret
```bash
kubectl create namespace clickgraph-test
kubectl create secret generic clickhouse-creds \
  --from-literal=password='test_password' \
  -n clickgraph-test
```

### 5. Install Chart (Development Mode)
```bash
helm install clickgraph ./helm/clickgraph \
  --namespace clickgraph-test \
  --set replicaCount=1 \
  --set clickhouse.enabled=true \
  --set clickhouse.external.enabled=false \
  --wait --timeout 5m
```

### 6. Verify Deployment
```bash
# Check pods are running
kubectl get pods -n clickgraph-test

# Check service is created
kubectl get svc -n clickgraph-test

# Check logs
kubectl logs -n clickgraph-test -l app.kubernetes.io/name=clickgraph
```

### 7. Test Connectivity
```bash
# Port-forward to test
kubectl port-forward -n clickgraph-test svc/clickgraph 8080:8080

# In another terminal, test HTTP endpoint
curl http://localhost:8080/health
```

## Known Issues to Check

### Template Issues
- [ ] ConfigMap YAML indentation in `configmap.yaml` (toYaml filter)
- [ ] Environment variable GRAPH_CONFIG_PATH comma handling
- [ ] Secret reference paths in deployment
- [ ] Volume mount paths match container expectations

### Values Issues
- [ ] Default `clickhouse.external.existingSecret` must exist
- [ ] Schema YAML structure matches ClickGraph expectations
- [ ] Resource limits are appropriate
- [ ] Probe paths and ports match actual server

### Runtime Issues
- [ ] ClickGraph binary exists at expected path in container
- [ ] /tmp emptyDir mount allows temporary files
- [ ] Schema ConfigMap successfully mounted
- [ ] ClickHouse connection string format correct

## Production Testing

### 8. Install with Production Values
```bash
helm install clickgraph ./helm/clickgraph \
  --namespace clickgraph-prod \
  -f values-production.yaml \
  --wait --timeout 10m
```

### 9. Test Autoscaling
```bash
# Generate load and verify HPA scales
kubectl get hpa -n clickgraph-prod -w
```

### 10. Test High Availability
```bash
# Delete a pod and verify recovery
kubectl delete pod -n clickgraph-prod -l app.kubernetes.io/name=clickgraph --force --grace-period=0

# Check PDB prevents too many deletions
kubectl drain NODE_NAME --ignore-daemonsets
```

## Fixes Required

Document any issues found during testing:

### Issue 1: [Description]
- **Problem**: 
- **Fix**: 
- **Files**: 

### Issue 2: [Description]
- **Problem**: 
- **Fix**: 
- **Files**: 

## Sign-off

- [ ] All validation tests pass
- [ ] Installation succeeds
- [ ] Pods reach Ready state
- [ ] Health check returns 200
- [ ] Basic Cypher query executes
- [ ] Logs show no errors
- [ ] Autoscaling works (if enabled)
- [ ] PDB prevents disruption
- [ ] Documentation updated with findings

**Tested by**: _______________  
**Date**: _______________  
**Kubernetes version**: _______________  
**Helm version**: _______________
