import os
from django.utils import timezone
from migration_engine.domain.adapters.postgres import PostgresAdapter
from migration_engine.domain.adapters.sqlite import SQLiteAdapter
from migration_engine.domain.etl.transformer import transform
from migration_engine.models import MigrationRunLog


def _build_adapter(profile):
    if profile.db_type == "postgres":
        return PostgresAdapter(
            {
                "dbname": profile.database,
                "user": profile.username or os.getenv("DB_USER"),
                "password": profile.password or os.getenv("DB_PASS"),
                "host": profile.host or os.getenv("HOST", "localhost"),
                "port": profile.port or os.getenv("DB_PORT", "5432"),
            }
        )
    return SQLiteAdapter(profile.database)


def log(job, stage, message, level="INFO"):
    MigrationRunLog.objects.create(job=job, stage=stage, message=message, level=level)


def run_job(job):
    source = _build_adapter(job.source_profile)
    target = _build_adapter(job.target_profile)
    try:
        job.status = "TRANSFORMING"
        job.stage = "extract"
        job.started_at = timezone.now()
        job.save(update_fields=["status", "stage", "started_at", "updated_at"])

        source.connect()
        target.connect()

        source_schema = source.fetch_schema(job.old_table)
        data = source.fetch_all(job.old_table)
        job.rows_read = len(data)
        log(job, "extract", f"extracted {len(data)} rows")

        job.stage = "transform"
        transformed = transform(data, job.column_mapping)
        log(job, "transform", f"transformed {len(transformed)} rows")

        job.status = "LOADING"
        job.stage = "load"
        job.save(update_fields=["status", "stage", "rows_read", "updated_at"])

        target_schema = target.map_schema_for_target(source_schema, job.column_mapping)
        target.insert(job.new_table, transformed, schema=target_schema)
        job.rows_written = len(transformed)

        job.status = "VERIFYING"
        job.stage = "verify"
        log(job, "verify", "row-count validation passed" if job.rows_written == job.rows_read else "row-count warning", "INFO")

        job.status = "SUCCESS"
        job.stage = "complete"
        job.finished_at = timezone.now()
        job.save(update_fields=["status", "stage", "rows_written", "finished_at", "updated_at"])
        return job
    except Exception as exc:
        job.status = "FAILED"
        job.stage = "error"
        job.errors += 1
        job.finished_at = timezone.now()
        job.save(update_fields=["status", "stage", "errors", "finished_at", "updated_at"])
        log(job, "error", str(exc), "ERROR")
        raise
    finally:
        source.close()
        target.close()
