> **Note**: This documentation is for ClickGraph v0.6.1. [View latest docs →](../../wiki/Home.md)
# Kubernetes Deployment Guide

**Caution:** This entire document is AI-generated. It may contain mistakes. Double check and raise issues for correction if you find any.

⚠️ **IMPORTANT**: The Helm chart and Kubernetes manifests in this guide have NOT been tested yet. They are based on best practices but require validation before use. See `helm/clickgraph/TESTING_CHECKLIST.md` for testing procedures.

Complete guide for deploying ClickGraph on Kubernetes with Helm charts, scaling strategies, and production best practices.

## Table of Contents
- [Quick Start](#quick-start)
- [Helm Chart Installation](#helm-chart-installation)
- [Manual Kubernetes Deployment](#manual-kubernetes-deployment)
- [Configuration Options](#configuration-options)
- [Scaling and High Availability](#scaling-and-high-availability)
- [Monitoring and Observability](#monitoring-and-observability)
- [Security and RBAC](#security-and-rbac)
- [Troubleshooting](#troubleshooting)

---

## Quick Start

### Prerequisites

```bash
# Required tools
kubectl version --client  # v1.24+
helm version             # v3.10+

# Verify cluster access
kubectl cluster-info
kubectl get nodes
```

### 5-Minute Deployment

```bash
# 1. Add ClickGraph Helm repository
helm repo add clickgraph https://clickgraph.io/charts
helm repo update

# 2. Install ClickGraph with default settings
helm install my-clickgraph clickgraph/clickgraph \
  --namespace clickgraph \
  --create-namespace \
  --set clickhouse.enabled=true

# 3. Wait for pods to be ready
kubectl wait --for=condition=ready pod \
  -l app.kubernetes.io/name=clickgraph \
  -n clickgraph \
  --timeout=300s

# 4. Port-forward to access
kubectl port-forward -n clickgraph \
  svc/my-clickgraph 8080:8080 7687:7687

# 5. Test the deployment
curl http://localhost:8080/health
```

---

## Helm Chart Installation

### Install from Repository

```bash
# Basic installation
helm install clickgraph clickgraph/clickgraph \
  --namespace clickgraph \
  --create-namespace

# Installation with custom values
helm install clickgraph clickgraph/clickgraph \
  --namespace clickgraph \
  --create-namespace \
  --values values-production.yaml

# Installation with inline overrides
helm install clickgraph clickgraph/clickgraph \
  --namespace clickgraph \
  --create-namespace \
  --set replicaCount=3 \
  --set resources.requests.memory=4Gi \
  --set clickhouse.enabled=true \
  --set ingress.enabled=true \
  --set ingress.hosts[0].host=clickgraph.example.com
```

### Helm Values Structure

**Create `values-production.yaml`**:

```yaml
# ClickGraph Configuration
replicaCount: 3

image:
  repository: clickgraph/clickgraph
  tag: "0.5.0"
  pullPolicy: IfNotPresent

# Resource limits
resources:
  requests:
    cpu: 1000m
    memory: 2Gi
  limits:
    cpu: 4000m
    memory: 4Gi

# Environment variables
env:
  - name: CLICKHOUSE_URL
    value: "http://clickhouse:8123"
  - name: CLICKHOUSE_USER
    valueFrom:
      secretKeyRef:
        name: clickhouse-credentials
        key: username
  - name: CLICKHOUSE_PASSWORD
    valueFrom:
      secretKeyRef:
        name: clickhouse-credentials
        key: password
  - name: RUST_LOG
    value: "info"
  - name: MAX_RECURSION_DEPTH
    value: "100"

# Schema configuration
schemas:
  - name: social-graph
    configMap: social-graph-schema
    mountPath: /app/schemas/social.yaml

# Service configuration
service:
  type: ClusterIP
  http:
    port: 8080
  bolt:
    port: 7687

# Ingress configuration
ingress:
  enabled: true
  className: nginx
  annotations:
    cert-manager.io/cluster-issuer: letsencrypt-prod
    nginx.ingress.kubernetes.io/ssl-redirect: "true"
    nginx.ingress.kubernetes.io/rate-limit: "100"
  hosts:
    - host: clickgraph.example.com
      paths:
        - path: /
          pathType: Prefix
  tls:
    - secretName: clickgraph-tls
      hosts:
        - clickgraph.example.com

# Health checks
livenessProbe:
  httpGet:
    path: /health
    port: http
  initialDelaySeconds: 30
  periodSeconds: 10
  timeoutSeconds: 5
  failureThreshold: 3

readinessProbe:
  httpGet:
    path: /health
    port: http
  initialDelaySeconds: 10
  periodSeconds: 5
  timeoutSeconds: 3
  failureThreshold: 3

# Horizontal Pod Autoscaling
autoscaling:
  enabled: true
  minReplicas: 3
  maxReplicas: 10
  targetCPUUtilizationPercentage: 70
  targetMemoryUtilizationPercentage: 80

# Pod Disruption Budget
podDisruptionBudget:
  enabled: true
  minAvailable: 2

# Affinity and tolerations
affinity:
  podAntiAffinity:
    preferredDuringSchedulingIgnoredDuringExecution:
      - weight: 100
        podAffinityTerm:
          labelSelector:
            matchExpressions:
              - key: app.kubernetes.io/name
                operator: In
                values:
                  - clickgraph
          topologyKey: kubernetes.io/hostname

# Security context
securityContext:
  runAsNonRoot: true
  runAsUser: 1000
  fsGroup: 1000
  capabilities:
    drop:
      - ALL
  readOnlyRootFilesystem: true

# ClickHouse (embedded for testing)
clickhouse:
  enabled: true  # Set to false for external ClickHouse
  persistence:
    enabled: true
    size: 100Gi
    storageClass: fast-ssd
  resources:
    requests:
      cpu: 2000m
      memory: 8Gi
    limits:
      cpu: 8000m
      memory: 32Gi

# Monitoring
metrics:
  enabled: true
  serviceMonitor:
    enabled: true
    interval: 30s
    labels:
      prometheus: kube-prometheus
```

### Upgrade Deployment

```bash
# Upgrade to new version
helm upgrade clickgraph clickgraph/clickgraph \
  --namespace clickgraph \
  --values values-production.yaml \
  --set image.tag=0.5.1

# Rollback to previous version
helm rollback clickgraph -n clickgraph

# View release history
helm history clickgraph -n clickgraph
```

---

## Manual Kubernetes Deployment

### 1. Namespace and ConfigMap

**namespace.yaml**:
```yaml
apiVersion: v1
kind: Namespace
metadata:
  name: clickgraph
  labels:
    name: clickgraph
```

**schema-configmap.yaml**:
```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: social-graph-schema
  namespace: clickgraph
data:
  social.yaml: |
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
            email: email_address
      
      relationships:
        - type: FOLLOWS
          database: brahmand
          table: user_follows_bench
          from_id: follower_id
          to_id: followed_id
          from_node: User
          to_node: User
          property_mappings:
            follow_date: follow_date
```

### 2. Secrets

**secrets.yaml**:
```yaml
apiVersion: v1
kind: Secret
metadata:
  name: clickhouse-credentials
  namespace: clickgraph
type: Opaque
stringData:
  username: "clickgraph_user"
  password: "CHANGE_ME_STRONG_PASSWORD"
  url: "http://clickhouse.clickgraph.svc.cluster.local:8123"
```

**Create from command line**:
```bash
kubectl create secret generic clickhouse-credentials \
  --namespace clickgraph \
  --from-literal=username=clickgraph_user \
  --from-literal=password="$(openssl rand -base64 32)" \
  --from-literal=url=http://clickhouse:8123
```

### 3. Deployment

**deployment.yaml**:
```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: clickgraph
  namespace: clickgraph
  labels:
    app: clickgraph
spec:
  replicas: 3
  strategy:
    type: RollingUpdate
    rollingUpdate:
      maxSurge: 1
      maxUnavailable: 0
  selector:
    matchLabels:
      app: clickgraph
  template:
    metadata:
      labels:
        app: clickgraph
      annotations:
        prometheus.io/scrape: "true"
        prometheus.io/port: "9090"
        prometheus.io/path: "/metrics"
    spec:
      serviceAccountName: clickgraph
      securityContext:
        runAsNonRoot: true
        runAsUser: 1000
        fsGroup: 1000
      
      containers:
      - name: clickgraph
        image: clickgraph/clickgraph:0.5.0
        imagePullPolicy: IfNotPresent
        
        ports:
        - name: http
          containerPort: 8080
          protocol: TCP
        - name: bolt
          containerPort: 7687
          protocol: TCP
        - name: metrics
          containerPort: 9090
          protocol: TCP
        
        env:
        - name: RUST_LOG
          value: "info"
        - name: CLICKHOUSE_URL
          valueFrom:
            secretKeyRef:
              name: clickhouse-credentials
              key: url
        - name: CLICKHOUSE_USER
          valueFrom:
            secretKeyRef:
              name: clickhouse-credentials
              key: username
        - name: CLICKHOUSE_PASSWORD
          valueFrom:
            secretKeyRef:
              name: clickhouse-credentials
              key: password
        - name: GRAPH_CONFIG_PATH
          value: "/app/schemas/social.yaml"
        - name: MAX_RECURSION_DEPTH
          value: "100"
        
        volumeMounts:
        - name: schemas
          mountPath: /app/schemas
          readOnly: true
        - name: tmp
          mountPath: /tmp
        
        resources:
          requests:
            cpu: 1000m
            memory: 2Gi
          limits:
            cpu: 4000m
            memory: 4Gi
        
        livenessProbe:
          httpGet:
            path: /health
            port: http
          initialDelaySeconds: 30
          periodSeconds: 10
          timeoutSeconds: 5
          failureThreshold: 3
        
        readinessProbe:
          httpGet:
            path: /health
            port: http
          initialDelaySeconds: 10
          periodSeconds: 5
          timeoutSeconds: 3
          failureThreshold: 3
        
        securityContext:
          allowPrivilegeEscalation: false
          capabilities:
            drop:
              - ALL
          readOnlyRootFilesystem: true
      
      volumes:
      - name: schemas
        configMap:
          name: social-graph-schema
      - name: tmp
        emptyDir: {}
      
      affinity:
        podAntiAffinity:
          preferredDuringSchedulingIgnoredDuringExecution:
          - weight: 100
            podAffinityTerm:
              labelSelector:
                matchExpressions:
                - key: app
                  operator: In
                  values:
                  - clickgraph
              topologyKey: kubernetes.io/hostname
```

### 4. Service

**service.yaml**:
```yaml
apiVersion: v1
kind: Service
metadata:
  name: clickgraph
  namespace: clickgraph
  labels:
    app: clickgraph
spec:
  type: ClusterIP
  ports:
  - name: http
    port: 8080
    targetPort: http
    protocol: TCP
  - name: bolt
    port: 7687
    targetPort: bolt
    protocol: TCP
  - name: metrics
    port: 9090
    targetPort: metrics
    protocol: TCP
  selector:
    app: clickgraph
  sessionAffinity: None
```

### 5. Ingress

**ingress.yaml**:
```yaml
apiVersion: networking.k8s.io/v1
kind: Ingress
metadata:
  name: clickgraph
  namespace: clickgraph
  annotations:
    cert-manager.io/cluster-issuer: letsencrypt-prod
    nginx.ingress.kubernetes.io/ssl-redirect: "true"
    nginx.ingress.kubernetes.io/rate-limit: "100"
    nginx.ingress.kubernetes.io/proxy-body-size: "1m"
    nginx.ingress.kubernetes.io/proxy-read-timeout: "60"
    nginx.ingress.kubernetes.io/proxy-send-timeout: "60"
spec:
  ingressClassName: nginx
  tls:
  - hosts:
    - clickgraph.example.com
    secretName: clickgraph-tls
  rules:
  - host: clickgraph.example.com
    http:
      paths:
      - path: /
        pathType: Prefix
        backend:
          service:
            name: clickgraph
            port:
              name: http
```

### 6. ServiceAccount and RBAC

**rbac.yaml**:
```yaml
apiVersion: v1
kind: ServiceAccount
metadata:
  name: clickgraph
  namespace: clickgraph
---
apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata:
  name: clickgraph
  namespace: clickgraph
rules:
- apiGroups: [""]
  resources: ["configmaps", "secrets"]
  verbs: ["get", "list"]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: RoleBinding
metadata:
  name: clickgraph
  namespace: clickgraph
subjects:
- kind: ServiceAccount
  name: clickgraph
  namespace: clickgraph
roleRef:
  kind: Role
  name: clickgraph
  apiGroup: rbac.authorization.k8s.io
```

### Deploy All Resources

```bash
# Apply all manifests
kubectl apply -f namespace.yaml
kubectl apply -f secrets.yaml
kubectl apply -f schema-configmap.yaml
kubectl apply -f rbac.yaml
kubectl apply -f deployment.yaml
kubectl apply -f service.yaml
kubectl apply -f ingress.yaml

# Or apply entire directory
kubectl apply -f k8s/
```

---

## Configuration Options

### Environment Variables

```yaml
env:
  # Required
  - name: CLICKHOUSE_URL
    value: "http://clickhouse:8123"
  - name: CLICKHOUSE_USER
    value: "clickgraph_user"
  - name: CLICKHOUSE_PASSWORD
    valueFrom:
      secretKeyRef:
        name: clickhouse-credentials
        key: password
  
  # Schema configuration
  - name: GRAPH_CONFIG_PATH
    value: "/app/schemas/social.yaml,/app/schemas/commerce.yaml"
  
  # Server configuration
  - name: HTTP_PORT
    value: "8080"
  - name: BOLT_PORT
    value: "7687"
  - name: HTTP_ENABLED
    value: "true"
  - name: BOLT_ENABLED
    value: "true"
  
  # Query execution
  - name: MAX_RECURSION_DEPTH
    value: "100"
  - name: QUERY_TIMEOUT_SECS
    value: "60"
  
  # Logging
  - name: RUST_LOG
    value: "info,clickgraph=debug"
  - name: LOG_FORMAT
    value: "json"
```

### Multi-Schema Configuration

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: multi-schema-config
  namespace: clickgraph
data:
  social.yaml: |
    name: social_graph
    # ... social schema ...
  
  commerce.yaml: |
    name: commerce_graph
    # ... commerce schema ...
  
  knowledge.yaml: |
    name: knowledge_graph
    # ... knowledge schema ...
---
# Mount all schemas
volumeMounts:
- name: schemas
  mountPath: /app/schemas
  readOnly: true

volumes:
- name: schemas
  configMap:
    name: multi-schema-config

# Load all schemas
env:
- name: GRAPH_CONFIG_PATH
  value: "/app/schemas/social.yaml,/app/schemas/commerce.yaml,/app/schemas/knowledge.yaml"
```

---

## Scaling and High Availability

### Horizontal Pod Autoscaler

**hpa.yaml**:
```yaml
apiVersion: autoscaling/v2
kind: HorizontalPodAutoscaler
metadata:
  name: clickgraph
  namespace: clickgraph
spec:
  scaleTargetRef:
    apiVersion: apps/v1
    kind: Deployment
    name: clickgraph
  minReplicas: 3
  maxReplicas: 10
  metrics:
  - type: Resource
    resource:
      name: cpu
      target:
        type: Utilization
        averageUtilization: 70
  - type: Resource
    resource:
      name: memory
      target:
        type: Utilization
        averageUtilization: 80
  behavior:
    scaleDown:
      stabilizationWindowSeconds: 300
      policies:
      - type: Percent
        value: 50
        periodSeconds: 60
    scaleUp:
      stabilizationWindowSeconds: 0
      policies:
      - type: Percent
        value: 100
        periodSeconds: 30
      - type: Pods
        value: 2
        periodSeconds: 30
      selectPolicy: Max
```

### Pod Disruption Budget

**pdb.yaml**:
```yaml
apiVersion: policy/v1
kind: PodDisruptionBudget
metadata:
  name: clickgraph
  namespace: clickgraph
spec:
  minAvailable: 2
  selector:
    matchLabels:
      app: clickgraph
```

### Multi-AZ Deployment

```yaml
affinity:
  # Spread across availability zones
  podAntiAffinity:
    requiredDuringSchedulingIgnoredDuringExecution:
    - labelSelector:
        matchExpressions:
        - key: app
          operator: In
          values:
          - clickgraph
      topologyKey: topology.kubernetes.io/zone
  
  # Prefer spreading across nodes
  podAntiAffinity:
    preferredDuringSchedulingIgnoredDuringExecution:
    - weight: 100
      podAffinityTerm:
        labelSelector:
          matchExpressions:
          - key: app
            operator: In
            values:
            - clickgraph
        topologyKey: kubernetes.io/hostname

# Node selector for specific node pools
nodeSelector:
  workload-type: compute-intensive

# Tolerations for tainted nodes
tolerations:
- key: "workload-type"
  operator: "Equal"
  value: "compute-intensive"
  effect: "NoSchedule"
```

---

## Monitoring and Observability

### Prometheus ServiceMonitor

**servicemonitor.yaml**:
```yaml
apiVersion: monitoring.coreos.com/v1
kind: ServiceMonitor
metadata:
  name: clickgraph
  namespace: clickgraph
  labels:
    app: clickgraph
    prometheus: kube-prometheus
spec:
  selector:
    matchLabels:
      app: clickgraph
  endpoints:
  - port: metrics
    interval: 30s
    path: /metrics
    scheme: http
```

### Grafana Dashboard ConfigMap

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: clickgraph-dashboard
  namespace: monitoring
  labels:
    grafana_dashboard: "1"
data:
  clickgraph-dashboard.json: |
    {
      "dashboard": {
        "title": "ClickGraph Metrics",
        "panels": [
          {
            "title": "Request Rate",
            "targets": [{
              "expr": "rate(clickgraph_requests_total[5m])"
            }]
          },
          {
            "title": "Request Latency (p95)",
            "targets": [{
              "expr": "histogram_quantile(0.95, rate(clickgraph_request_duration_seconds_bucket[5m]))"
            }]
          }
        ]
      }
    }
```

### Logging with Fluentd

```yaml
# Add logging sidecar
containers:
- name: fluentd
  image: fluent/fluentd:v1.15
  volumeMounts:
  - name: varlog
    mountPath: /var/log
  - name: fluentd-config
    mountPath: /fluentd/etc
```

---

## Security and RBAC

### Network Policies

**networkpolicy.yaml**:
```yaml
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata:
  name: clickgraph
  namespace: clickgraph
spec:
  podSelector:
    matchLabels:
      app: clickgraph
  policyTypes:
  - Ingress
  - Egress
  
  ingress:
  # Allow ingress from nginx ingress controller
  - from:
    - namespaceSelector:
        matchLabels:
          name: ingress-nginx
    ports:
    - protocol: TCP
      port: 8080
    - protocol: TCP
      port: 7687
  
  # Allow ingress from Prometheus
  - from:
    - namespaceSelector:
        matchLabels:
          name: monitoring
    ports:
    - protocol: TCP
      port: 9090
  
  egress:
  # Allow egress to ClickHouse
  - to:
    - podSelector:
        matchLabels:
          app: clickhouse
    ports:
    - protocol: TCP
      port: 8123
    - protocol: TCP
      port: 9000
  
  # Allow DNS
  - to:
    - namespaceSelector: {}
      podSelector:
        matchLabels:
          k8s-app: kube-dns
    ports:
    - protocol: UDP
      port: 53
```

### Pod Security Policy (deprecated in K8s 1.25+)

For K8s < 1.25, use PSP. For 1.25+, use **Pod Security Standards**.

**pod-security-standard.yaml**:
```yaml
apiVersion: v1
kind: Namespace
metadata:
  name: clickgraph
  labels:
    pod-security.kubernetes.io/enforce: restricted
    pod-security.kubernetes.io/audit: restricted
    pod-security.kubernetes.io/warn: restricted
```

---

## Troubleshooting

### Check Pod Status

```bash
# List all pods
kubectl get pods -n clickgraph

# Describe pod for events
kubectl describe pod -n clickgraph <pod-name>

# View logs
kubectl logs -n clickgraph <pod-name>
kubectl logs -n clickgraph <pod-name> --previous  # Previous instance

# Follow logs
kubectl logs -n clickgraph -f <pod-name>

# Logs from all replicas
kubectl logs -n clickgraph -l app=clickgraph --tail=100
```

### Common Issues

#### 1. Pods in CrashLoopBackOff

```bash
# Check logs
kubectl logs -n clickgraph <pod-name> --previous

# Common causes:
# - Missing secrets (CLICKHOUSE_PASSWORD)
# - Invalid schema YAML
# - Cannot connect to ClickHouse
# - Insufficient resources

# Fix: Check environment variables
kubectl get secret -n clickgraph clickhouse-credentials -o yaml
```

#### 2. Readiness Probe Failing

```bash
# Check health endpoint manually
kubectl port-forward -n clickgraph <pod-name> 8080:8080
curl http://localhost:8080/health

# Common causes:
# - ClickHouse not accessible
# - Schema failed to load
# - Long startup time (increase initialDelaySeconds)
```

#### 3. High Memory Usage

```bash
# Check resource usage
kubectl top pod -n clickgraph

# View OOMKilled events
kubectl get events -n clickgraph --field-selector reason=OOMKilled

# Fix: Increase memory limits
kubectl set resources deployment/clickgraph \
  -n clickgraph \
  --limits=memory=8Gi \
  --requests=memory=4Gi
```

#### 4. Schema Not Loading

```bash
# Verify ConfigMap exists
kubectl get configmap -n clickgraph social-graph-schema

# Check YAML syntax
kubectl get configmap -n clickgraph social-graph-schema -o yaml

# Verify mount path
kubectl exec -n clickgraph <pod-name> -- ls -la /app/schemas/

# Check logs for schema errors
kubectl logs -n clickgraph <pod-name> | grep -i schema
```

### Debug Commands

```bash
# Execute shell in pod
kubectl exec -it -n clickgraph <pod-name> -- /bin/sh

# Test ClickHouse connectivity
kubectl exec -n clickgraph <pod-name> -- \
  curl -v http://clickhouse:8123

# Check environment variables
kubectl exec -n clickgraph <pod-name> -- env | grep CLICKHOUSE

# Port-forward for local testing
kubectl port-forward -n clickgraph svc/clickgraph 8080:8080 7687:7687
```

---

## Production Checklist

**Pre-Deployment**:
- [ ] Helm chart values reviewed and customized
- [ ] Secrets created with strong passwords
- [ ] Schema ConfigMaps validated
- [ ] Resource limits set appropriately
- [ ] Ingress configured with TLS
- [ ] Network policies defined
- [ ] RBAC permissions minimal

**High Availability**:
- [ ] At least 3 replicas configured
- [ ] HPA enabled and tested
- [ ] PodDisruptionBudget configured
- [ ] Anti-affinity rules for multi-AZ spread
- [ ] ClickHouse replication configured

**Monitoring**:
- [ ] ServiceMonitor configured
- [ ] Grafana dashboards imported
- [ ] Alerts configured (CPU, memory, error rate)
- [ ] Logging aggregation set up
- [ ] Health check endpoints verified

**Security**:
- [ ] Non-root user (1000:1000)
- [ ] Read-only root filesystem
- [ ] Capabilities dropped (drop: ALL)
- [ ] Network policies enforced
- [ ] Secrets stored securely (not in Git!)
- [ ] TLS enabled for all ingress

---

## Next Steps

Now that ClickGraph is deployed on Kubernetes:

- **[Production Best Practices](Production-Best-Practices.md)** - Security and operations
- **[Monitoring Guide](Monitoring-Observability.md)** - Set up comprehensive monitoring
- **[Performance Tuning](Performance-Query-Optimization.md)** - Optimize for your workload
- **[Troubleshooting Guide](Troubleshooting-Guide.md)** - Debug common issues

---

[← Back: Docker Deployment](Docker-Deployment.md) | [Home](Home.md) | [Next: Monitoring →](Monitoring-Observability.md)
