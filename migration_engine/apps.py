from django.apps import AppConfig


class MigrationEngineConfig(AppConfig):
    default_auto_field = "django.db.models.BigAutoField"
    name = "migration_engine"
