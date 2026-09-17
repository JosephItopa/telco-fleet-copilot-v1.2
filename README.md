# telco-fleet-copilot (v1.2)

Kafka service that scans **every application on every configured Kubernetes
cluster** — production fleets of 1000+ pods — and publishes one record per pod
per scan to a Kafka topic for downstream consumers (alerting, capacity, SLO
reporting, copilot/RAG ingestion).

- Scan cadence: every **3 minutes** by default (`SCAN_INTERVAL_SECONDS=180`).
- Pod/state data comes from the Kubernetes API; runtime metrics come from
  Prometheus. Each cluster can have its own Prometheus.
- Publishing is batched and keyed by `cluster/namespace/service/pod`, so every
  pod's history stays ordered on one partition.
- Query count is **constant per scan** (`~1` instant query per metric), not one
  query per app, so 1000+ apps cost the same Prometheus budget as 10.

## Architecture

```
        ┌──────────────────────────────────────────────────────────┐
        │                    fleet-copilot scanner                 │
        │                                                          │
  ┌─────▼──────┐   list pods    ┌────────────────┐   PromQL        │
  │ Cluster A  │◄───────────────┤ Kubernetes     │   instant queries│
  │ Cluster B  │                │ collector      │◄────────────────┤
  │ Cluster N  │                │ (async, thread │  ┌────────────┐ │
  └────────────┘                │  per cluster)  │  │ Prometheus │ │
                                └───────┬────────┘  │  A/B/N     │ │
                                        │           └────────────┘ │
                                ┌───────▼────────┐                 │
                                │ FleetScanner   │  merge K8s state│
                                │ (resolve+emit) │  + Prometheus   │
                                └───────┬────────┘                 │
                                ┌───────▼────────┐                 │
                                │ Kafka producer │  one batch/flush│
                                └───────┬────────┘                 │
                                        │                          │
        └───────────────────────────────┼──────────────────────────┘
                                        ▼
                              topic: fleet.app.metrics
                                        │
                     ┌──────────────────┼──────────────────┐
                     ▼                  ▼                  ▼
                alerting           capacity            copilot/RAG
```

Modules:

| Path | Responsibility |
| --- | --- |
| `src/fleet_copilot/config.py` | Env-driven configuration, cluster list loading |
| `src/fleet_copilot/models.py` | `AppRef` (discovered pod) and `AppMetric` (wire contract) |
| `src/fleet_copilot/metrics_schema.py` | PromQL definitions + `PROMETHEUS_METRIC_QUERIES_FILE` overrides |
| `src/fleet_copilot/collectors/kubernetes.py` | Lists pods per cluster, derives readiness/liveness/restarts |
| `src/fleet_copilot/collectors/prometheus.py` | Batched instant queries, in-memory `MetricsIndex` |
| `src/fleet_copilot/scanner.py` | Orchestrates one scan and merges sources |
| `src/fleet_copilot/producer.py` | `confluent-kafka` batch producer (+ `NullProducer` dry run) |
| `src/fleet_copilot/service.py` | 3-minute loop with drift-free scheduling |
| `src/fleet_copilot/consumer.py` | Reference consumer with example alert thresholds |
| `src/fleet_copilot/mocks.py` | Deterministic 1200-app mock fleet (`MOCK_MODE=true`) |

## Payload contract

One JSON object per pod per scan, keyed by `cluster/namespace/service/pod`:

```json
{
  "timestamp": "2026-09-17T12:00:00Z",
  "service": "payment-api",
  "namespace": "production",
  "pod": "payment-api-7d9f8",
  "cpu_usage_percent": 82.4,
  "memory_usage_percent": 91.2,
  "cpu_throttling_percent": 14.3,
  "request_rate": 245,
  "error_rate_percent": 7.8,
  "p95_latency_ms": 1240,
  "active_requests": 53,
  "restart_count": 3,
  "pod_ready": true,
  "disk_usage_percent": 76.2,
  "db_latency_ms": 480,
  "db_connection_pool_percent": 87,
  "liveness": true,
  "readiness": true,
  "cluster": "prod-eu"
}
```

`cluster` is the only addition to the agreed sample; it is required because the
scanner covers multiple clusters. Consumers that ignore unknown fields are
unaffected. Metrics with no Prometheus series are emitted as `null`, never
fabricated as `0`. `restart_count`, `pod_ready`, `liveness` and `readiness`
always come from the Kubernetes API, never from Prometheus.

Field sources:

| Field | Source | Default metric |
| --- | --- | --- |
| `cpu_usage_percent` | Prometheus (pod) | `container_cpu_usage_seconds_total` / CPU limit |
| `memory_usage_percent` | Prometheus (pod) | `container_memory_working_set_bytes` / memory limit |
| `cpu_throttling_percent` | Prometheus (pod) | `container_cpu_cfs_throttled_periods_total` |
| `request_rate` | Prometheus (service) | `http_requests_total` |
| `error_rate_percent` | Prometheus (service) | 5xx share of `http_requests_total` |
| `p95_latency_ms` | Prometheus (service) | `http_request_duration_seconds_bucket` |
| `active_requests` | Prometheus (service) | `http_requests_in_flight` |
| `restart_count` | Kubernetes | container statuses |
| `pod_ready` / `readiness` | Kubernetes | `Ready` condition / container ready |
| `liveness` | Kubernetes | phase + waiting/terminated reasons |
| `disk_usage_percent` | Prometheus (pod) | `container_fs_usage_bytes` / `container_fs_limit_bytes` |
| `db_latency_ms` | Prometheus (service) | `db_query_duration_seconds_sum/count` |
| `db_connection_pool_percent` | Prometheus (service) | `db_pool_connections_used/max` |

## Quickstart (mock fleet, no cluster needed)

```bash
docker compose up --build
```

This starts a single-node Kafka and a scanner in `MOCK_MODE` that publishes a
deterministic 1200-pod fleet every 3 minutes.

Consume the stream:

```bash
docker compose exec kafka kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic fleet.app.metrics --from-beginning --max-messages 3
```

Or with the reference consumer locally:

```bash
pip install -r requirements-dev.txt
KAFKA_BOOTSTRAP_SERVERS=localhost:9092 python -m fleet_copilot.consumer --from-beginning --limit 20
```

One-shot scans are useful for debugging and backfills:

```bash
python -m fleet_copilot --mock --once --dry-run --echo
```

## Real clusters

1. Apply `deploy/rbac.yaml` on each cluster (the scanner needs cluster-wide
   `list pods`).
2. Provide credentials. Either run the scanner as a pod in one cluster and
   point `CLUSTERS_FILE` at a kubeconfig for the others, or mount one
   kubeconfig with multiple contexts (see `clusters.example.yaml`).

```bash
export KAFKA_BOOTSTRAP_SERVERS=kafka-1:9092,kafka-2:9092,kafka-3:9092
export CLUSTERS_FILE=/etc/fleet-copilot/clusters.yaml
export PROMETHEUS_METRIC_QUERIES_FILE=/etc/fleet-copilot/metrics.json
python -m fleet_copilot
```

### Prometheus query overrides

Every query is overridable, because metric names are organisation specific:

```json
[
  {"field": "request_rate", "scope": "service",
   "query": "sum by (namespace, service) (rate(istio_requests_total[5m]))"},
  {"field": "p95_latency_ms", "scope": "service",
   "query": "1000 * histogram_quantile(0.95, sum by (namespace, service, le) (rate(istio_request_duration_milliseconds_bucket[5m])))"}
]
```

Rules:

- `field` must be one of the metric fields in the table above (`restart_count`,
  `pod_ready`, `liveness`, `readiness` are Kubernetes-owned and rejected).
- `scope` is `pod` or `service`. Pod-scoped results are indexed by
  `(namespace, pod)`; service-scoped results by `(namespace, <app label>)`,
  where the label is taken from `service`, `app`, `app_kubernetes_io_name`,
  `k8s_app`, then `job`.
- Resolution falls back from service to pod scope, so a service-level metric
  still resolves for pods with no matching series.

## Configuration reference

| Variable | Default | Notes |
| --- | --- | --- |
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | Comma-separated brokers |
| `KAFKA_TOPIC` | `fleet.app.metrics` | Auto-create in dev; pre-create in prod |
| `KAFKA_CLIENT_ID` | `fleet-copilot-scanner` | |
| `KAFKA_SECURITY_PROTOCOL` / `KAFKA_SASL_*` | unset | Set together for SASL_SSL |
| `KAFKA_ENABLE_IDEMPOTENCE` | `true` | Forces `acks=all` |
| `KAFKA_LINGER_MS` / `KAFKA_BATCH_SIZE` | `50` / `65536` | Producer batching |
| `SCAN_INTERVAL_SECONDS` | `180` | 3 minutes |
| `SCAN_CONCURRENCY` | `64` | Worker/connection pool sizing |
| `NAMESPACE_ALLOW` | empty (all) | Comma allowlist |
| `NAMESPACE_DENY` | `kube-system,...` | Comma denylist |
| `REQUIRE_OWNER` | `true` | Ignore unmanaged pods |
| `PROMETHEUS_URL` | unset | Fallback for clusters without one |
| `PROMETHEUS_TIMEOUT_SECONDS` | `20` | Per-query timeout |
| `PROMETHEUS_METRIC_QUERIES_FILE` | unset | JSON query overrides |
| `CLUSTERS` | unset | Inline JSON cluster list |
| `CLUSTERS_FILE` | unset | JSON/YAML cluster list |
| `MOCK_MODE` | `false` | Deterministic synthetic fleet |
| `LOG_LEVEL` | `INFO` | |

## Scaling notes

- **Discovery**: one `list_pod_for_all_namespaces` per cluster per scan; each
  cluster is listed in its own thread with an isolated `ApiClient`, so clusters
  never share mutable global client state.
- **Metrics**: one instant PromQL query per metric per cluster, then O(1)
  in-memory lookups while assembling records. 10 metrics × N clusters, not
  10 × 1000 apps.
- **Publishing**: records are produced as one batch and flushed once per scan.
  Keying by pod preserves per-pod ordering without partitioning per service.
- **Failure isolation**: a failing cluster or PromQL query degrades to `null`
  metrics instead of failing the scan; the loop logs and continues.
- **Topic sizing**: at 1200 pods × 20 scans/hour ≈ 24k records/hour ≈ 0.6 MB/min
  at ~1.4 KB/record. Partition count should be ≥ consumer parallelism; 12–24
  partitions is comfortable for this volume.

## Development

```bash
pip install -r requirements-dev.txt
pytest
ruff check .
```
