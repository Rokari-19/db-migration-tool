# ETL API Contract (MVP)

This document defines a concrete REST API contract for integrating the ETL pipeline into the Django project.

## Conventions

- Base URL: `/api/v1`
- Auth: Bearer token (JWT)
- Content-Type: `application/json`
- Long-running operations return `202 Accepted` with a `job_id`.
- `status` values: `PENDING | PLANNING | PROFILING | MAPPING | TRANSFORMING | LOADING | VERIFYING | SUCCESS | FAILED | ROLLED_BACK`

## 1) Test Source/Target Connections

### `POST /api/v1/migrations/connections/test`

Checks if source and target connection configs are valid.

#### Request
```json
{
  "source": {
    "db_type": "sqlite",
    "database": "db.sqlite3"
  },
  "target": {
    "db_type": "postgres",
    "host": "localhost",
    "port": 5432,
    "database": "migration_db",
    "username": "postgres",
    "password": "***",
    "ssl_mode": "prefer"
  }
}
```

#### Response 200
```json
{
  "ok": true,
  "source": {"ok": true, "message": "connection successful"},
  "target": {"ok": true, "message": "connection successful"}
}
```

---

## 2) Create Plan

### `POST /api/v1/migrations/plan`

Creates a migration job and plan metadata.

#### Request
```json
{
  "name": "sqlite_to_postgres_users",
  "source_profile_id": 1,
  "target_profile_id": 2,
  "tables": ["users"],
  "batch_size": 500,
  "stop_on_error": true
}
```

#### Response 201
```json
{
  "job_id": "3f9fb5b1-0c8f-4d64-aa58-d8fd764b49be",
  "status": "PENDING",
  "plan": {
    "tables": ["users"],
    "batch_size": 500,
    "stop_on_error": true
  }
}
```

---

## 3) Profile Schemas

### `POST /api/v1/migrations/{job_id}/profile`

Introspects source and target schemas and stores abstract model.

#### Request
```json
{}
```

#### Response 200
```json
{
  "job_id": "3f9fb5b1-0c8f-4d64-aa58-d8fd764b49be",
  "status": "PROFILING",
  "profile": {
    "source_tables": [
      {
        "name": "users",
        "columns": [
          {"name": "customer_name", "type": "TEXT", "nullable": false},
          {"name": "customer_email", "type": "TEXT", "nullable": false}
        ]
      }
    ],
    "target_tables": []
  }
}
```

---

## 4) Generate/Submit Mapping Rules

### `POST /api/v1/migrations/{job_id}/map`

Creates mapping rules (manual rules for MVP, optional suggestions).

#### Request
```json
{
  "rules": [
    {
      "source_table": "users",
      "target_table": "users",
      "source_column": "customer_name",
      "target_column": "full_name",
      "transform": "identity"
    },
    {
      "source_table": "users",
      "target_table": "users",
      "source_column": "customer_email",
      "target_column": "email_address",
      "transform": "lowercase"
    }
  ]
}
```

#### Response 200
```json
{
  "job_id": "3f9fb5b1-0c8f-4d64-aa58-d8fd764b49be",
  "status": "MAPPING",
  "rules_saved": 2
}
```

---

## 5) Run Migration

### `POST /api/v1/migrations/{job_id}/run`

Dispatches background task to execute `plan -> profile -> map -> transform -> load -> verify`.

#### Request
```json
{
  "resume": false
}
```

#### Response 202
```json
{
  "job_id": "3f9fb5b1-0c8f-4d64-aa58-d8fd764b49be",
  "status": "PENDING",
  "task_id": "celery-9f95f1b1"
}
```

---

## 6) Get Migration Status

### `GET /api/v1/migrations/{job_id}/status`

#### Response 200
```json
{
  "job_id": "3f9fb5b1-0c8f-4d64-aa58-d8fd764b49be",
  "status": "LOADING",
  "stage": "load",
  "progress": {
    "percent": 72,
    "rows_read": 7200,
    "rows_written": 7000,
    "errors": 2
  },
  "started_at": "2026-05-09T10:11:12Z",
  "updated_at": "2026-05-09T10:13:33Z"
}
```

---

## 7) Get Logs

### `GET /api/v1/migrations/{job_id}/logs?level=INFO&limit=100`

#### Response 200
```json
{
  "job_id": "3f9fb5b1-0c8f-4d64-aa58-d8fd764b49be",
  "count": 3,
  "results": [
    {"ts": "2026-05-09T10:12:00Z", "stage": "transform", "level": "INFO", "message": "processed 500 rows"},
    {"ts": "2026-05-09T10:12:08Z", "stage": "load", "level": "WARNING", "message": "2 rows skipped: null email"},
    {"ts": "2026-05-09T10:12:30Z", "stage": "verify", "level": "INFO", "message": "row-count validation passed"}
  ]
}
```

---

## 8) Rollback (Optional in MVP)

### `POST /api/v1/migrations/{job_id}/rollback`

#### Request
```json
{
  "reason": "manual abort"
}
```

#### Response 202
```json
{
  "job_id": "3f9fb5b1-0c8f-4d64-aa58-d8fd764b49be",
  "status": "ROLLED_BACK"
}
```

---

## Error Contract

For all non-2xx responses:

```json
{
  "error": {
    "code": "VALIDATION_ERROR",
    "message": "Invalid mapping rule",
    "details": {
      "field": "rules[1].target_column",
      "reason": "target column does not exist"
    }
  }
}
```

Common codes:

- `VALIDATION_ERROR`
- `AUTH_REQUIRED`
- `PERMISSION_DENIED`
- `CONNECTION_FAILED`
- `SCHEMA_PROFILE_FAILED`
- `MIGRATION_FAILED`
- `ROLLBACK_FAILED`

