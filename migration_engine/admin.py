from django.contrib import admin

from .models import ConnectionProfile, MigrationJob, MigrationRunLog


@admin.register(ConnectionProfile)
class ConnectionProfileAdmin(admin.ModelAdmin):
    list_display = ["name", "db_type", "database", "host", "port", "created_at"]
    list_filter = ["db_type", "created_at"]
    search_fields = ["name", "database", "host", "username"]
    fieldsets = (
        (None, {"fields": ("name", "db_type")}),
        (
            "Connection",
            {
                "fields": (
                    "uri",
                    "database",
                    "host",
                    "port",
                    "username",
                    "password",
                    "ssl_mode",
                ),
                "description": "For MongoDB Atlas, paste your full pymongo URI in 'uri'. The 'database' field should match the target DB name.",
            },
        ),
    )


@admin.register(MigrationJob)
class MigrationJobAdmin(admin.ModelAdmin):
    list_display = ["id", "name", "status", "stage", "rows_read", "rows_written", "errors", "updated_at"]
    list_filter = ["status", "stage", "created_at", "updated_at"]
    search_fields = ["name", "old_table", "new_table"]


@admin.register(MigrationRunLog)
class MigrationRunLogAdmin(admin.ModelAdmin):
    list_display = ["id", "job", "stage", "level", "ts"]
    list_filter = ["level", "stage", "ts"]
    search_fields = ["job__name", "message"]
