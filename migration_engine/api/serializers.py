from rest_framework import serializers
from migration_engine.models import ConnectionProfile, MigrationJob, MigrationRunLog


class HostedDBConfigSerializer(serializers.Serializer):
    provider = serializers.ChoiceField(choices=["render", "vercel", "railway", "netlify"])
    resource_id = serializers.CharField(max_length=255)
    api_url = serializers.URLField(required=False, allow_blank=True, default="")
    api_token = serializers.CharField(required=False, allow_blank=True, default="")
    metadata = serializers.JSONField(required=False, default=dict)


class DBConnectionConfigSerializer(serializers.Serializer):
    db_type = serializers.ChoiceField(choices=["sqlite", "postgres", "mongodb"])
    database = serializers.CharField(max_length=255)
    host = serializers.CharField(required=False, allow_blank=True, default="")
    port = serializers.IntegerField(required=False, allow_null=True)
    username = serializers.CharField(required=False, allow_blank=True, default="")
    password = serializers.CharField(required=False, allow_blank=True, default="")
    uri = serializers.CharField(required=False, allow_blank=True, default="")
    ssl_mode = serializers.CharField(required=False, allow_blank=True, default="prefer")
    hosted_db = HostedDBConfigSerializer(required=False)


class ConnectionProfileSerializer(serializers.ModelSerializer):
    class Meta:
        model = ConnectionProfile
        fields = [
            "id",
            "name",
            "db_type",
            "database",
            "host",
            "port",
            "username",
            "password",
            "uri",
            "ssl_mode",
            "hosted_provider",
            "hosted_resource_id",
            "hosted_api_url",
            "hosted_api_token",
            "hosted_metadata",
            "created_at",
        ]


class ConnectionTestSerializer(serializers.Serializer):
    source = DBConnectionConfigSerializer()
    target = DBConnectionConfigSerializer()


class MigrationPlanSerializer(serializers.Serializer):
    name = serializers.CharField(max_length=150)
    source_profile_id = serializers.IntegerField()
    target_profile_id = serializers.IntegerField()
    old_table = serializers.CharField(max_length=120)
    new_table = serializers.CharField(max_length=120)
    column_mapping = serializers.JSONField(required=False, default=dict)
    batch_size = serializers.IntegerField(default=500)
    stop_on_error = serializers.BooleanField(default=True)


class MigrationJobStatusSerializer(serializers.ModelSerializer):
    source_profile_name = serializers.CharField(source="source_profile.name", read_only=True)
    target_profile_name = serializers.CharField(source="target_profile.name", read_only=True)

    class Meta:
        model = MigrationJob
        fields = [
            "id",
            "name",
            "status",
            "stage",
            "old_table",
            "new_table",
            "source_profile",
            "source_profile_name",
            "target_profile",
            "target_profile_name",
            "rows_read",
            "rows_written",
            "errors",
            "started_at",
            "updated_at",
            "finished_at",
        ]


class MigrationLogSerializer(serializers.ModelSerializer):
    class Meta:
        model = MigrationRunLog
        fields = ["ts", "stage", "level", "message"]
