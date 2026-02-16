# KubeTimer – Copilot Instructions

## What This Is

A **Kopf-based Kubernetes operator** that deletes resources whose ISO 8601 TTL annotation (`kubetimer.io/ttl`) has expired. Configuration is driven by a cluster-scoped CRD (`KubeTimerConfig`). Currently only Deployments are implemented; Pods and ReplicaSets are planned.

## Architecture & Data Flow

```
KubeTimerConfig CRD → kopf.Memo (shared state) → Timer handler (periodic) → Resource handler → K8s API delete
```

- **`main.py`** – Registers all Kopf handlers/indexes at import time via `register_all_handlers()`, then runs the operator with `uvloop`.
- **`handlers/registry.py`** – Owns `kopf.Memo` initialization (`init_memo`) and population (`configure_memo`). Registers Kopf indexes for each resource type.
- **`handlers/timer.py`** – `check_ttl_timer_handler` is the Kopf timer entry point; reads memo config and dispatches to per-resource handlers.
- **`handlers/deployment.py`** – `deployment_indexer` builds a Kopf index keyed by name. `deployment_handler` iterates the index snapshot, checks TTL expiry, and deletes via `AppsV1Api`.
- **`config/k8s.py`** – `KubeTimerConfig` dataclass, K8s client factories, and CRD fetching (`get_kubetimerconfig`).
- **`config/settings.py`** – Pydantic Settings with `KUBETIMER_` env prefix (e.g. `KUBETIMER_LOG_LEVEL`).
- **`utils/time_utils.py`** – ISO 8601 parsing and timezone-aware expiry checks.
- **`utils/logs.py`** – structlog setup; `setup_logging()` is `@lru_cache`-ed.

## Key Patterns

- **Kopf indexes** – Resources are indexed via `kopf.index()` decorators rather than listing from the API on every scan. New resource types need an indexer function + registration in `register_all_indexes`.
- **`kopf.Memo` as shared state** – All runtime config (enabled resources, annotation key, dry_run, namespaces, timezone) lives on `memo`. Always use `configure_memo()` to populate it.
- **Handler registration** – Kopf handlers are registered imperatively in `register_all_handlers()` (not via top-level decorators) so they can reference the pre-initialized memo.
- **Structured logging** – Use `structlog` with snake_case event names (e.g. `"deployment_deleted"`). Get loggers via `get_logger(__name__)`.
- **CRD group/version** – Always `kubetimer.io/v1`. The singleton config resource is named `kubetimerconfig`.

## Adding a New Resource Type (e.g. Pods)

1. Create `handlers/pods.py` with a `pod_indexer` function and a `pod_handler` (follow `deployment.py`).
2. Register the index in `registry.py → register_all_indexes`.
3. Call the handler from `timer.py → check_ttl_timer_handler` when `'pods' in enabled_resources`.
4. Export from `handlers/__init__.py`.

## Dev Workflow

```bash
uv sync                        # install deps (uv is the package manager)
uv pip install -e .            # editable install (or `make install`)
python kubetimer/main.py       # run locally (needs kubeconfig + CRD applied)
./deploy.sh                    # deploy to cluster (applies namespace, CRD, RBAC, Deployment)
```

- **Python 3.14**, managed with **uv** and **hatchling** build backend.
- Docker image uses a multi-stage build from `ghcr.io/astral-sh/uv:python3.14-trixie-slim`.
- No test suite yet — `tests/` contains load-generation and measurement scripts.

## Environment Variables

| Variable | Default | Purpose |
|---|---|---|
| `KUBETIMER_LOG_LEVEL` | `INFO` | App log level |
| `KUBETIMER_KOPF_LOG_LEVEL` | `WARNING` | Kopf framework log level |
| `KUBETIMER_LOG_FORMAT` | `text` | `json` for production, `text` for dev |
| `KUBETIMER_CHECK_INTERVAL` | `60` | Seconds between TTL scans (5–3600) |
