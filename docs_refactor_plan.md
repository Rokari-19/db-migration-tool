# First Refactor Patch Plan (ETL -> Django Integration)

This plan maps the current repository files to a Django-native modular architecture while keeping implementation incremental and testable.

## Current Files

- `engine.py`
- `adapters.py`
- `config/settings.py`
- `config/urls.py`
- `manage.py`

## Target Architecture (Phase 1)

Create a new Django app: `migration_engine/`.

```text
migration_engine/
  __init__.py
  apps.py
  models.py
  admin.py
  api/
    __init__.py
    serializers.py
    views.py
    urls.py
  domain/
    __init__.py
    adapters/
      __init__.py
      base.py
      postgres.py
      sqlite.py
    etl/
      __init__.py
      extractor.py
      transformer.py
      loader.py
      verifier.py
  application/
    __init__.py
    workflow.py
    services.py
  infrastructure/
    __init__.py
    logging.py
    tasks.py
```

---

## File-by-file Mapping

## 1) `adapters.py` -> split into domain adapters

### Move:
- `BaseAdapter` -> `migration_engine/domain/adapters/base.py`
- `PostgresAdapter` -> `migration_engine/domain/adapters/postgres.py`
- `SQLiteAdapter` -> `migration_engine/domain/adapters/sqlite.py`

### First improvements:
- standardize placeholders (`%s` for psycopg2, `?` for sqlite)
- add context-managed connection lifecycle
- avoid hardcoded column names in `insert()`

---

## 2) `engine.py` -> split ETL and orchestration

### Move:
- `transform()` -> `migration_engine/domain/etl/transformer.py`
- `run()` -> `migration_engine/application/workflow.py`
- CLI-only parsing logic remains temporary (for backward compatibility) in `engine.py`

### New flow:
- `workflow.execute(job_id)`
  - load job config
  - build source/target adapters
  - extract
  - transform
  - load
  - verify
  - update metrics and logs

---

## 3) Create API surface

### Add:
- `migration_engine/api/serializers.py`
- `migration_engine/api/views.py`
- `migration_engine/api/urls.py`

### Endpoints in first patch:
- `POST /api/v1/migrations/connections/test`
- `POST /api/v1/migrations/plan`
- `POST /api/v1/migrations/{job_id}/run`
- `GET /api/v1/migrations/{job_id}/status`

### Integrate routes:
- include app urls in `config/urls.py`

---

## 4) Add persistence models

### `migration_engine/models.py`

Add minimal MVP models:
- `ConnectionProfile`
- `MigrationJob`
- `MigrationMappingRule`
- `MigrationRunLog`
- `MigrationMetric`

First patch can store credentials encrypted or as placeholders with TODO for encryption key management.

---

## 5) Async execution

### `migration_engine/infrastructure/tasks.py`

- add Celery task `run_migration_job(job_id)`
- `/run` endpoint enqueues task
- `/status` reads `MigrationJob` state + progress fields

First patch can stub task execution sync if worker not set up yet, but preserve task API.

---

## 6) Settings and app registration

### Update `config/settings.py`

- add `migration_engine` to `INSTALLED_APPS`
- configure DRF auth/permissions defaults
- optional: add Celery/Redis config placeholders

---

## 7) Backward compatibility strategy

Keep `engine.py` working as CLI wrapper during transition:
- it should call `migration_engine.application.services.run_once(...)`
- mark file deprecated in header comment

This avoids breaking current usage while APIs are introduced.

---

## Milestone Breakdown

## Milestone A (Patch 1)
- Scaffold `migration_engine` app
- Move adapters + transform logic
- Add core models
- Add `/connections/test`, `/plan`, `/run`, `/status`
- Basic logging table writes

## Milestone B (Patch 2)
- Implement schema profiling endpoint
- Implement mapping endpoint + rule validation
- Add batch migration + failure policies

## Milestone C (Patch 3)
- Add verification report model and endpoint
- Add rollback mechanics
- Add audit log hardening and permission scopes

---

## Definition of Done for Patch 1

- New app created and registered
- Existing CLI script still works
- API can trigger a migration job and report status
- Adapter code no longer hardcodes `full_name`/`email_address`
- Unit tests cover transformer + workflow happy path

