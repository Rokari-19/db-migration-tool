import os
from django.utils import timezone
from migration_engine.domain.adapters.postgres import PostgresAdapter
from migration_engine.domain.adapters.sqlite import SQLiteAdapter
from migration_engine.domain.adapters.mongodb import MongoDBAdapter
from migration_engine.domain.etl.transformer import transform
from migration_engine.infrastructure import HostedDBRef, get_hosted_db_wrapper
from migration_engine.models import MigrationRunLog


def _resolve_hosted(profile):
    if not getattr(profile, "hosted_provider", ""):
        return {}
    reference = HostedDBRef(
        provider=profile.hosted_provider,
        resource_id=profile.hosted_resource_id,
        api_token=profile.hosted_api_token,
        api_url=profile.hosted_api_url,
        metadata=profile.hosted_metadata or {},
    )
    return get_hosted_db_wrapper(profile.hosted_provider).resolve(reference)


def _build_adapter(profile):
    resolved = _resolve_hosted(profile)
    if profile.db_type == "postgres":
        return PostgresAdapter(
            {
                "dbname": profile.database or resolved.get("database") or os.getenv("PGDATABASE") or os.getenv("DB_NAME"),
                "user": profile.username or resolved.get("username") or os.getenv("PGUSER") or os.getenv("DB_USER"),
                "password": profile.password or resolved.get("password") or os.getenv("PGPASSWORD") or os.getenv("DB_PASS"),
                "host": profile.host or resolved.get("host") or os.getenv("PGHOST") or os.getenv("HOST", "localhost"),
                "port": profile.port or resolved.get("port") or os.getenv("PGPORT") or os.getenv("DB_PORT", "5432"),
            }
        )
    if profile.db_type == "mongodb":
        return MongoDBAdapter(
            {
                "uri": profile.uri or resolved.get("uri") or os.getenv("MONGODB_URI", ""),
                "database": profile.database or resolved.get("database"),
                "username": profile.username or resolved.get("username") or os.getenv("DB_USER"),
                "password": profile.password or resolved.get("password") or os.getenv("DB_PASS"),
                "host": profile.host or resolved.get("host") or os.getenv("HOST", "localhost"),
                "port": profile.port or resolved.get("port") or os.getenv("DB_PORT", "27017"),
                "ssl_mode": profile.ssl_mode or resolved.get("ssl_mode") or "prefer",
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

        if job.old_table == "*":
            table_pairs = [(table_name, table_name) for table_name in source.list_tables()]
        else:
            table_pairs = [(job.old_table, job.new_table)]

        total_rows_read = 0
        total_rows_written = 0

        for source_table, target_table in table_pairs:
            source_schema = source.fetch_schema(source_table)
            data = source.fetch_all(source_table)
            total_rows_read += len(data)
            log(job, "extract", f"extracted {len(data)} rows from {source_table}")

            if job.column_mapping:
                column_mapping = job.column_mapping
            else:
                source_columns = list(source_schema.keys()) or list(data[0].keys()) if data else []
                column_mapping = {column: column for column in source_columns}

            job.stage = "transform"
            transformed = transform(data, column_mapping)
            log(job, "transform", f"transformed {len(transformed)} rows for {source_table}")

            job.status = "LOADING"
            job.stage = "load"
            job.save(update_fields=["status", "stage", "updated_at"])

            target_schema = target.map_schema_for_target(source_schema, column_mapping)
            target.insert(target_table, transformed, schema=target_schema)
            total_rows_written += len(transformed)

        job.rows_read = total_rows_read
        job.rows_written = total_rows_written

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
