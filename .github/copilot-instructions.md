# KubeTimer – Copilot Instructions

## What This Is

A **Kopf-based Kubernetes operator** that deletes Deployments whose ISO 8601 TTL annotation (`kubetimer.io/ttl`) has expired. Deletions are **event-driven** via APScheduler `DateTrigger` jobs — not periodic polling. Configuration comes from `KUBETIMER_*` environment variables (Pydantic Settings). Python 3.14, managed with **uv** and **hatchling** build backend.

## Architecture & Data Flow

```
K8s watch events → Kopf event handlers → APScheduler DateTrigger jobs → K8s API delete
                                                                ↑
Startup: list API (paginated) → reconcile orchestrator → bulk_delete / schedule_deletion_job
```

### Key modules

- **`kubetimer/__init__.py`** – `register_all_handlers()` imperatively registers all Kopf handlers (`on.startup`, `on.cleanup`, `on.create`, `on.field`, `on.delete`). Also contains `startup_handler` (loads config, starts APScheduler, runs reconciliation) and `shutdown_handler`.
- **`handlers/deployment.py`** – Three thin event handlers: `on_deployment_created_with_ttl`, `on_ttl_annotation_changed`, `on_deployment_deleted_with_ttl`. They validate, check namespace filters, then delegate to `scheduler/jobs.py`.
- **`handlers/registry.py`** – Only `configure_memo()` — populates `kopf.Memo` from Settings.
- **`scheduler/jobs.py`** – APScheduler job management: `schedule_deletion_job` (DateTrigger), `cancel_deletion_job`, `delete_deployment_job` (re-verifies UID + TTL before deleting).
- **`reconcile/`** – Startup recovery: `orchestrator.py` fetches all TTL-annotated Deployments, triages into expired vs future, bulk-deletes expired ones. Uses `fetcher.py` (paginated list API, sync/async delete wrappers), `bulk_delete.py` (semaphore-bounded concurrent deletes), `models.py` (`TtlDeployment` frozen dataclass).
- **`config/settings.py`** – `Settings(BaseSettings)` with `KUBETIMER_` env prefix. `get_settings()` is `@lru_cache`-ed.
- **`config/k8s.py`** – K8s client loading, `@lru_cache`-ed `apps_v1_client()`, connection pool sizing.
- **`utils/time_utils.py`** – `parse_ttl()` (ISO 8601 → datetime), `is_ttl_expired()`.
- **`utils/namespace.py`** – `should_scan_namespace()` with include/exclude filtering.
- **`utils/logs.py`** – structlog setup; `setup_logging()` is `@lru_cache`-ed.
- **`main.py`** – Sets up `uvloop`, calls `register_all_handlers()`, runs `kopf.run()`.

## Key Patterns

- **Event-driven, not polling** – Kopf watches trigger handlers on create/update/delete. APScheduler `DateTrigger` fires a one-shot job at the exact TTL expiry time. No periodic scanning.
- **`kopf.Memo` as shared state** – Runtime config (annotation_key, dry_run, timezone, namespace filters, scheduler, reconciling_uids) lives on `memo`. Populated by `configure_memo()` in `startup_handler`.
- **Imperative handler registration** – All `kopf.on.*` calls happen in `register_all_handlers()` (in `__init__.py`), not via top-level decorators, so they can reference module-level settings.
- **Reconciling UIDs guard** – `memo.reconciling_uids: set[str]` prevents event handlers from double-processing Deployments during startup reconciliation. UIDs are added before reconciliation, discarded as each job completes.
- **Re-verification before delete** – `delete_deployment_job` re-reads the Deployment from the API and checks UID match + TTL still expired before deleting. This guards against recreated resources or annotation changes.
- **Sync-in-async via `asyncio.to_thread`** – The kubernetes client is synchronous. All blocking API calls are wrapped with `asyncio.to_thread` to keep the event loop free.
- **Structured logging** – Use `structlog` with snake_case event names (e.g. `"deployment_deleted_by_scheduler"`). Get loggers via `get_logger(__name__)`.

## Dev Workflow

```bash
uv sync                        # install deps
make install                   # editable install (uv pip install -e .)
python kubetimer/main.py       # run locally (needs kubeconfig)
./deploy.sh                    # deploy to cluster (namespace, CRD, RBAC, Deployment)
make test                      # run unit tests (uv run --group dev pytest)
make coverage                  # pytest with coverage report
make check                     # format-check + lint + typecheck + test
make fix                       # auto-format + lint
```

## Testing Patterns

- Tests live in `tests/unit/`. Config: `asyncio_mode = "auto"`, `pythonpath = ["."]`.
- `conftest.py` provides a `memo` fixture using `SimpleNamespace` (not real `kopf.Memo`) with a `MagicMock` scheduler that simulates `AsyncIOScheduler` (start/shutdown toggle `running`, `add_job` returns mock job).
- Handlers are tested by patching their downstream calls (`schedule_deletion_job`, `async_delete_namespaced_deployment`, `cancel_deletion_job`). Tests call handler functions directly with the `memo` fixture.
- `FUTURE_TTL` / `PAST_TTL` are computed relative to `datetime.now(timezone.utc)` at test module load time.

## Operational Testing Tools (`tests/`)

Scripts for live cluster testing — all require a working kubeconfig and a running operator.

| Script | Purpose |
|---|---|
| `generate_zombies.py` | Creates zero-replica "zombie" Deployments at a controlled rate with randomised TTLs (mix of past and future). Use `--past-ratio` to tune the expired fraction and `--cleanup` to remove on exit. |
| `generate_load.py` | Bulk-creates 2 000 expired Deployments in batches of 200 to stress-test startup reconciliation throughput. |
| `measure.py` | Waits for zombie Deployments to appear, then times how long the operator takes to delete them all (deletion throughput benchmark). |
| `monitor_logs.py` | Streams the operator pod's logs in real time, parsing structured events and printing a summary of event counts, deletion throughput, reconciliation duration, and errors on exit. |
| `monitor_resources.py` | Polls the Metrics API every N seconds and prints a min/avg/max/p95 CPU and memory report with an ASCII chart. Requires `metrics-server` (`minikube addons enable metrics-server`). |
| `cleanup_zombies.py` | Deletes all `app=kubetimer-zombie` Deployments, optionally across all namespaces, with concurrency control and finalizer removal. |

**Typical workflow:**
```bash
# 1. Start the operator
./deploy.sh

# 2. Generate load (e.g. 50% expired, 2 deploys/sec for 60s, cleanup on exit)
python tests/generate_zombies.py --duration 60 --past-ratio 0.5 --cleanup

# 3. Monitor in parallel terminals
python tests/monitor_logs.py --duration 60
python tests/monitor_resources.py --duration 60

# 4. Benchmark reconciliation bulk delete
python tests/generate_load.py   # creates 2000 expired deployments
python tests/measure.py         # times operator clearing them
```

## Adding a New Resource Type (e.g. Pods)

1. Create `handlers/pod.py` with `on_pod_created_with_ttl`, `on_ttl_annotation_changed` (for pods), `on_pod_deleted_with_ttl` — follow `handlers/deployment.py`.
2. Add K8s API wrappers in `reconcile/fetcher.py` (e.g. `delete_namespaced_pod`, paginated list).
3. Register Kopf handlers in `kubetimer/__init__.py → register_all_handlers()` using `kopf.on.create("", "v1", "pods", ...)`.
4. Add pod reconciliation in `reconcile/orchestrator.py`.
5. Export from `handlers/__init__.py`.
6. Add unit tests in `tests/unit/test_pod_handlers.py`.

## Environment Variables

| Variable | Default | Purpose |
|---|---|---|
| `KUBETIMER_LOG_LEVEL` | `INFO` | App log level |
| `KUBETIMER_KOPF_LOG_LEVEL` | `WARNING` | Kopf framework log level |
| `KUBETIMER_LOG_FORMAT` | `text` | `json` for production, `text` for dev |
| `KUBETIMER_DRY_RUN` | `false` | Log deletions without deleting |
| `KUBETIMER_TIMEZONE` | `UTC` | IANA timezone for TTL comparison |
| `KUBETIMER_ANNOTATION_KEY` | `kubetimer.io/ttl` | Annotation key to watch |
| `KUBETIMER_NAMESPACE_EXCLUDE` | `kube-system,kube-public,kube-node-lease` | Namespaces to skip |
| `KUBETIMER_MAX_CONCURRENT_DELETES` | `25` | Concurrent K8s delete calls (1–200) |
| `KUBETIMER_CONNECTION_POOL_SIZE` | `50` | urllib3 pool + ThreadPoolExecutor workers |
| `KUBETIMER_LIST_PAGE_SIZE` | `1000` | Page size for paginated K8s list calls |
