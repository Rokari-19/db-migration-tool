from rest_framework import serializers
from migration_engine.models import ConnectionProfile, MigrationJob, MigrationRunLog


class ConnectionTestSerializer(serializers.Serializer):
    source = serializers.JSONField()
    target = serializers.JSONField()


class MigrationPlanSerializer(serializers.Serializer):
    name = serializers.CharField(max_length=150)
    source_profile_id = serializers.IntegerField()
    target_profile_id = serializers.IntegerField()
    old_table = serializers.CharField(max_length=120)
    new_table = serializers.CharField(max_length=120)
    column_mapping = serializers.JSONField()
    batch_size = serializers.IntegerField(default=500)
    stop_on_error = serializers.BooleanField(default=True)


class MigrationJobStatusSerializer(serializers.ModelSerializer):
    class Meta:
        model = MigrationJob
        fields = ["id", "name", "status", "stage", "rows_read", "rows_written", "errors", "started_at", "updated_at", "finished_at"]


class MigrationLogSerializer(serializers.ModelSerializer):
    class Meta:
        model = MigrationRunLog
        fields = ["ts", "stage", "level", "message"]
