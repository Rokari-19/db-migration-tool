# Database Migration Tool - Complete Documentation

## Overview

This is a **Django-based Database Migration Tool** that provides ETL (Extract-Transform-Load) functionality to migrate data between SQLite and PostgreSQL databases. It follows a clean architecture pattern with domain, application, and API layers.

---

## Architecture

```
Domain Layer (Business Logic)
├─ Adapters: Database abstraction (SQLite, PostgreSQL)
└─ ETL: Data transformation logic

Application Layer (Use Cases)
├─ Services: Business workflow orchestration
└─ Workflow: Execution coordinator

API Layer (HTTP Endpoints)
├─ Views: REST endpoints
└─ Serializers: Request/response validation
```

---

## Core Models

### 1. ConnectionProfile – Database connection configuration

- Stores source/target database credentials
- Supports SQLite and PostgreSQL
- Fields: `name`, `db_type`, `database`, `host`, `port`, `username`, `password`, `ssl_mode`
- Use case: Define reusable connection profiles for source and target databases

### 2. MigrationJob – Migration execution unit

- Tracks a data migration operation from source to target table
- Status lifecycle: `PENDING` → `PLANNING` → `PROFILING` → `MAPPING` → `TRANSFORMING` → `LOADING` → `VERIFYING` → `SUCCESS` (or `FAILED`)
- Fields: `column_mapping` (JSON), `batch_size`, `stop_on_error`, `rows_read`, `rows_written`, `errors`
- Use case: Represents a single migration task with all metadata and progress tracking

### 3. MigrationRunLog – Audit trail

- Logs each stage of the migration with timestamps
- Fields: `stage`, `level` (INFO/WARNING/ERROR), `message`
- Use case: Provides complete audit trail for debugging and monitoring

---

## API Endpoints

### 1. Test Database Connections

```
POST /migrations/connections/test
```

**Purpose:** Validate that source and target databases are accessible before running migrations.

**Request body:**

```json
{
  "source": {
    "db_type": "sqlite",
    "database": "/path/to/source.db"
  },
  "target": {
    "db_type": "postgres",
    "database": "target_db",
    "host": "localhost",
    "port": 5432,
    "username": "user",
    "password": "pass"
  }
}
```

**Data accepted:**

- `source.db_type`: Either "sqlite" or "postgres"
- `source.database`: Path to SQLite file or database name for PostgreSQL
- `source.host`: Optional, defaults to "localhost"
- `source.port`: Optional, defaults to 5432 for PostgreSQL
- `source.username`, `source.password`: Optional, defaults to empty strings
- Same structure for `target` object

**Response:**

```json
{
  "ok": true,
  "source": { "ok": true, "message": "connection successful" },
  "target": { "ok": true, "message": "connection successful" }
}
```

---

### 2. Plan/Create a Migration Job

```
POST /migrations/plan
```

**Purpose:** Create a new migration job with column mappings and configuration.

**Request body:**

```json
{
  "name": "users_migration_v1",
  "source_profile_id": 1,
  "target_profile_id": 2,
  "old_table": "users",
  "new_table": "users_new",
  "column_mapping": {
    "id": "user_id",
    "email": "email_address",
    "name": "full_name"
  },
  "batch_size": 500,
  "stop_on_error": true
}
```

**Data accepted:**

- `name`: Unique identifier for the migration job (max 150 chars)
- `source_profile_id`: ID of ConnectionProfile for source database
- `target_profile_id`: ID of ConnectionProfile for target database
- `old_table`: Source table name to migrate from
- `new_table`: Target table name to migrate to
- `column_mapping`: JSON object mapping source columns to target columns (e.g., `{"old_col": "new_col"}`)
- `batch_size`: Number of rows to process at once (default 500)
- `stop_on_error`: Whether to stop on first error or continue (default true)

**Response:**

```json
{
  "job_id": "42",
  "status": "PENDING"
}
```

**What this does:**

- Creates a MigrationJob record in database
- Validates that both ConnectionProfiles exist
- Sets initial status to PENDING
- Returns job ID for subsequent operations

---

### 3. Execute a Migration Job

```
POST /migrations/<job_id>/run
```

**Purpose:** Start the migration process (async operation).

**Parameters:**

- `job_id` (URL parameter): The ID of the migration job to execute

**Response:**

```json
{
  "job_id": "42",
  "status": "TRANSFORMING"
}
```

**What it does internally:**

1. Connects to source database using ConnectionProfile credentials
2. Fetches all rows from the source table (`old_table`)
3. Transforms data using column mapping from MigrationJob
4. Connects to target database
5. Inserts transformed data into target table (`new_table`)
6. Logs each stage with row counts
7. Updates job status to `SUCCESS` or `FAILED`
8. Disconnects from both databases

**Error handling:**

- If any error occurs, job status is set to FAILED
- Error count incremented
- Exception message logged
- Both database connections closed in finally block

---

### 4. Get Migration Job Status

```
GET /migrations/<job_id>/status
```

**Purpose:** Check current progress and results of a migration job.

**Parameters:**

- `job_id` (URL parameter): The ID of the migration job

**Response:**

```json
{
  "id": 42,
  "name": "users_migration_v1",
  "status": "SUCCESS",
  "stage": "complete",
  "rows_read": 1250,
  "rows_written": 1200,
  "errors": 0,
  "started_at": "2026-05-09T10:30:00Z",
  "updated_at": "2026-05-09T10:32:15Z",
  "finished_at": "2026-05-09T10:32:15Z"
}
```

**Response fields:**

- `status`: Current job status (PENDING, PLANNING, PROFILING, MAPPING, TRANSFORMING, LOADING, VERIFYING, SUCCESS, FAILED)
- `stage`: Current execution stage (extract, transform, load, verify, complete, error)
- `rows_read`: Total rows extracted from source
- `rows_written`: Total rows inserted to target
- `errors`: Count of rows that failed transformation (NULL values)

---

### 5. Get Migration Logs

```
GET /migrations/<job_id>/logs
```

**Purpose:** Retrieve audit trail for a migration job (last 100 logs, reverse chronological order).

**Parameters:**

- `job_id` (URL parameter): The ID of the migration job

**Response:**

```json
{
  "job_id": "42",
  "count": 4,
  "results": [
    {
      "ts": "2026-05-09T10:32:15Z",
      "stage": "complete",
      "level": "INFO",
      "message": "row-count validation passed"
    },
    {
      "ts": "2026-05-09T10:31:50Z",
      "stage": "load",
      "level": "INFO",
      "message": "inserted rows"
    },
    {
      "ts": "2026-05-09T10:30:05Z",
      "stage": "extract",
      "level": "INFO",
      "message": "extracted 1250 rows"
    }
  ]
}
```

**Log levels:**

- `INFO`: Normal operation messages
- `WARNING`: Potential issues (e.g., row count mismatches)
- `ERROR`: Exceptions and failures

---

## Data Flow & Processing

```
ConnectionProfile (source) ──┐
                              ├─→ BaseAdapter ──→ fetch_all() ──┐
                              │                                  │
ConnectionProfile (target) ──┤                                  ├─→ transform() ──→ insert()
                              │                                  │
MigrationJob.column_mapping ──┴─────────────────────────────────┘
```

### Transform Logic Details

- Maps columns from source to target using `column_mapping` dictionary
- Drops rows where any mapped column has a NULL value (by default)
- Only columns in `column_mapping` are included in output
- Data types are preserved as returned by database drivers
- Example: If mapping is `{"old_id": "id", "old_name": "name"}` and a row has NULL in `old_id`, that entire row is skipped

---

## Component Architecture

### Domain Layer - Database Adapters

**BaseAdapter** (base.py)

- Abstract base class for all database adapters
- Methods: `connect()`, `fetch_all(table_name)`, `insert(table_name, data)`, `close()`
- Placeholder strategy: `%s` (PostgreSQL) vs `?` (SQLite)
- Generic insert implementation using placeholder substitution

**SQLiteAdapter** (sqlite.py)

- Implementation for SQLite databases
- Uses `sqlite3` module
- Placeholder: `?`
- Implementation: Connects to local .db file, executes queries

**PostgresAdapter** (postgres.py)

- Implementation for PostgreSQL databases
- Uses `psycopg2` module
- Placeholder: `%s`
- Implementation: Connects via connection parameters (host, port, user, password, dbname)

### ETL Layer - Data Transformation

**transformer.py - transform(data, column_mapping, drop_nones=True)**

- Input: List of row dictionaries from source database
- Processing:
  - Iterates through each row
  - For each mapped source column → target column, extracts value
  - If `drop_nones=True` and any value is None, entire row is skipped
  - Otherwise, creates new dictionary with target column names
- Output: List of transformed row dictionaries ready for insertion
- Usage: Core of ETL pipeline, filters and remaps data

### Application Layer - Business Logic

**services.py - run_job(job)**

- Orchestrates complete migration pipeline
- Steps:
  1. Build adapters from ConnectionProfiles
  2. Set job status to TRANSFORMING, stage to extract
  3. Connect to source and target databases
  4. Fetch all rows from source table
  5. Transform data using column mapping
  6. Insert into target table
  7. Update job with row counts
  8. Set status to SUCCESS
  9. Log each stage
- Error handling: Catches exceptions, sets status to FAILED, logs error message
- Finally block: Always closes both database connections

**services.py - \_build_adapter(profile)**

- Factory function to create appropriate adapter based on profile type
- For PostgreSQL: Uses environment variables for missing credentials (DB_USER, DB_PASS, HOST, DB_PORT)
- For SQLite: Just needs database path

**services.py - log(job, stage, message, level="INFO")**

- Creates MigrationRunLog entry for audit trail
- Parameters: job reference, stage name, message, log level

**workflow.py - execute(job)**

- Thin wrapper around run_job()
- Entry point for migration execution

### API Layer - HTTP Interface

**serializers.py**

- `ConnectionTestSerializer`: Validates source/target database configs (JSONField for flexibility)
- `MigrationPlanSerializer`: Validates migration job creation request with all parameters
- `MigrationJobStatusSerializer`: Serializes job status for API response (subset of fields)
- `MigrationLogSerializer`: Serializes log entries with timestamp, stage, level, message

**views.py**

- `ConnectionTestAPIView`: Tests both source and target connections in parallel
- `MigrationPlanAPIView`: Creates new MigrationJob in database
- `MigrationRunAPIView`: Triggers job execution
- `MigrationStatusAPIView`: Returns job progress and stats
- `MigrationLogsAPIView`: Returns last 100 logs ordered newest first

---

## Testing Guide

### Prerequisites

```bash
# Install dependencies
pip install django djangorestframework psycopg2-binary

# Setup database
python manage.py migrate

# Create superuser for admin
python manage.py createsuperuser

# Start development server
python manage.py runserver
```

### Test 1: Create Connection Profiles (via Django Admin)

1. Navigate to http://localhost:8000/admin
2. Log in with superuser credentials
3. Go to Connection Profiles section
4. Create "local_sqlite" profile:
   - Name: local_sqlite
   - DB Type: sqlite
   - Database: /tmp/source.db
5. Create "prod_postgres" profile:
   - Name: prod_postgres
   - DB Type: postgres
   - Database: mydb
   - Host: localhost
   - Port: 5432
   - Username: postgres
   - Password: [your password]

### Test 2: Test Connections

```bash
curl -X POST http://localhost:8000/migrations/connections/test \
  -H "Content-Type: application/json" \
  -d '{
    "source": {
      "db_type": "sqlite",
      "database": "/tmp/source.db"
    },
    "target": {
      "db_type": "postgres",
      "database": "mydb",
      "host": "localhost",
      "port": 5432,
      "username": "postgres",
      "password": "password123"
    }
  }'
```

Expected: Both connections should return `"ok": true`

### Test 3: Create Migration Plan

```bash
curl -X POST http://localhost:8000/migrations/plan \
  -H "Content-Type: application/json" \
  -d '{
    "name": "test_users_migration",
    "source_profile_id": 1,
    "target_profile_id": 2,
    "old_table": "users",
    "new_table": "users_new",
    "column_mapping": {
      "id": "id",
      "name": "name",
      "email": "email"
    },
    "batch_size": 500,
    "stop_on_error": true
  }'
```

Expected response: Job created with ID and PENDING status. Note the job_id for next steps.

### Test 4: Execute Migration

```bash
curl -X POST http://localhost:8000/migrations/1/run
```

Expected: Job status changes to TRANSFORMING or SUCCESS

### Test 5: Check Job Status

```bash
curl http://localhost:8000/migrations/1/status
```

Expected: Shows rows_read, rows_written, current stage, and status

### Test 6: View Migration Logs

```bash
curl http://localhost:8000/migrations/1/logs
```

Expected: Shows all stages (extract, transform, load, verify, complete) with timestamps and messages

### Python Integration Test Script

```python
import requests
import json
import time

BASE_URL = "http://localhost:8000"

def test_migration_workflow():
    # 1. Test connections
    print("=== Testing Connections ===")
    test_payload = {
        "source": {"db_type": "sqlite", "database": "/tmp/source.db"},
        "target": {
            "db_type": "postgres",
            "database": "target_db",
            "host": "localhost",
            "port": 5432,
            "username": "postgres",
            "password": "pass"
        }
    }
    resp = requests.post(f"{BASE_URL}/migrations/connections/test", json=test_payload)
    print("Connection test:", resp.json())
    assert resp.json()["ok"], "Connection test failed"

    # 2. Create migration plan
    print("\n=== Creating Migration Plan ===")
    plan_payload = {
        "name": "users_test_" + str(int(time.time())),
        "source_profile_id": 1,
        "target_profile_id": 2,
        "old_table": "users",
        "new_table": "users_migrated",
        "column_mapping": {"user_id": "id", "user_name": "name", "user_email": "email"},
        "batch_size": 1000,
        "stop_on_error": False
    }
    resp = requests.post(f"{BASE_URL}/migrations/plan", json=plan_payload)
    result = resp.json()
    job_id = result["job_id"]
    print(f"Created job: {job_id} with status {result['status']}")
    assert resp.status_code == 201, "Failed to create migration plan"

    # 3. Run migration
    print(f"\n=== Running Migration Job {job_id} ===")
    resp = requests.post(f"{BASE_URL}/migrations/{job_id}/run")
    print("Migration started:", resp.json())
    assert resp.status_code == 202, "Failed to start migration"

    # 4. Poll status
    print(f"\n=== Polling Status ===")
    for i in range(10):
        resp = requests.get(f"{BASE_URL}/migrations/{job_id}/status")
        status_data = resp.json()
        print(f"Poll {i+1}: Status={status_data['status']}, Rows Read={status_data.get('rows_read', 0)}, Rows Written={status_data.get('rows_written', 0)}")
        if status_data['status'] in ['SUCCESS', 'FAILED']:
            break
        time.sleep(1)

    # 5. Get final logs
    print(f"\n=== Final Logs ===")
    resp = requests.get(f"{BASE_URL}/migrations/{job_id}/logs")
    logs = resp.json()
    for log in logs["results"]:
        print(f"[{log['ts']}] {log['stage']}: {log['message']}")

    print("\n=== Test Complete ===")

if __name__ == "__main__":
    test_migration_workflow()
```

---

## Key Implementation Details

| Component                  | Purpose                                | Location                      |
| -------------------------- | -------------------------------------- | ----------------------------- |
| **BaseAdapter**            | Abstract base for database connections | `domain/adapters/base.py`     |
| **SQLiteAdapter**          | SQLite-specific adapter                | `domain/adapters/sqlite.py`   |
| **PostgresAdapter**        | PostgreSQL-specific adapter            | `domain/adapters/postgres.py` |
| **transform()**            | Column mapping + NULL filtering        | `domain/etl/transformer.py`   |
| **run_job()**              | Orchestrates ETL pipeline              | `application/services.py`     |
| **execute()**              | Entry point for migration              | `application/workflow.py`     |
| **ConnectionTestAPIView**  | Connection testing endpoint            | `api/views.py`                |
| **MigrationPlanAPIView**   | Job creation endpoint                  | `api/views.py`                |
| **MigrationRunAPIView**    | Job execution endpoint                 | `api/views.py`                |
| **MigrationStatusAPIView** | Status tracking endpoint               | `api/views.py`                |
| **MigrationLogsAPIView**   | Audit trail endpoint                   | `api/views.py`                |

---

## Design Patterns Used

1. **Adapter Pattern**: BaseAdapter abstraction for database-specific implementations
2. **Factory Pattern**: `_build_adapter()` creates appropriate adapter based on profile type
3. **Strategy Pattern**: Different SQL placeholder strategies for SQLite vs PostgreSQL
4. **Repository Pattern**: Models act as repositories for job and log data
5. **Service Layer Pattern**: Services handle business logic separate from API views
6. **Serializer Pattern**: DRF serializers validate and transform data

---

## Error Handling & Edge Cases

1. **NULL Values in Mapped Columns**: Entire rows are dropped (by default)
2. **Database Connection Failures**: Caught and logged, job marked as FAILED
3. **Missing Credentials**: Falls back to environment variables or defaults
4. **Partial Failures**: If `stop_on_error=False`, migration continues despite failures
5. **Incomplete Mappings**: Only mapped columns are transferred; unmapped columns ignored
6. **Connection Cleanup**: Finally blocks ensure connections always close, even on error

---

## Performance Considerations

1. **Batch Processing**: Rows are processed in batches defined by `batch_size` (default 500)
2. **Memory Usage**: All rows fetched at once from source - consider for very large tables
3. **Index Impact**: Target table should have indexes on primary keys for fast inserts
4. **Transaction Handling**: Each insert wrapped in transaction (conn.commit per row group)
5. **Logging Overhead**: Each stage creates DB log entry - consider batching for large migrations

---

## Future Enhancement Opportunities

1. Streaming data processing (instead of fetch_all)
2. Batch insert optimization
3. Data type conversion and validation
4. Duplicate detection and handling
5. Partial retry capabilities
6. Progress percentage tracking
7. Support for more database types (MySQL, MongoDB, etc.)
8. Schema introspection and auto-mapping
9. Data validation rules
10. Rollback capabilities
