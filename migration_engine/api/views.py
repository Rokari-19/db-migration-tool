from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status
from migration_engine.application.workflow import execute
from migration_engine.domain.adapters.postgres import PostgresAdapter
from migration_engine.domain.adapters.sqlite import SQLiteAdapter
from migration_engine.domain.adapters.mongodb import MongoDBAdapter
from migration_engine.infrastructure import HostedDBRef, get_hosted_db_wrapper
from migration_engine.models import ConnectionProfile, MigrationJob
from .serializers import (
    ConnectionProfileSerializer,
    ConnectionTestSerializer,
    MigrationPlanSerializer,
    MigrationJobStatusSerializer,
    MigrationLogSerializer,
)


class ConnectionProfileAPIView(APIView):
    def get(self, request):
        profiles = ConnectionProfile.objects.order_by("name")
        return Response(ConnectionProfileSerializer(profiles, many=True).data)

    def post(self, request):
        serializer = ConnectionProfileSerializer(data=request.data)
        serializer.is_valid(raise_exception=True)
        profile = serializer.save()
        return Response(ConnectionProfileSerializer(profile).data, status=status.HTTP_201_CREATED)


class MigrationJobsAPIView(APIView):
    def get(self, request):
        jobs = MigrationJob.objects.select_related("source_profile", "target_profile").order_by("-updated_at")[:50]
        return Response(MigrationJobStatusSerializer(jobs, many=True).data)


class ConnectionTestAPIView(APIView):
    def post(self, request):
        serializer = ConnectionTestSerializer(data=request.data)
        serializer.is_valid(raise_exception=True)

        def resolve_hosted(conf):
            hosted_db = conf.get("hosted_db")
            if not hosted_db:
                return {}
            reference = HostedDBRef(
                provider=hosted_db["provider"],
                resource_id=hosted_db["resource_id"],
                api_token=hosted_db.get("api_token", ""),
                api_url=hosted_db.get("api_url", ""),
                metadata=hosted_db.get("metadata", {}),
            )
            return get_hosted_db_wrapper(hosted_db["provider"]).resolve(reference)

        def check(conf):
            try:
                resolved = resolve_hosted(conf)
                if conf["db_type"] == "sqlite":
                    adapter = SQLiteAdapter(conf["database"])
                elif conf["db_type"] == "mongodb":
                    adapter = MongoDBAdapter(
                        {
                            "uri": conf.get("uri") or resolved.get("uri", ""),
                            "database": conf.get("database") or resolved.get("database", ""),
                            "username": conf.get("username") or resolved.get("username", ""),
                            "password": conf.get("password") or resolved.get("password", ""),
                            "host": conf.get("host") or resolved.get("host", "localhost"),
                            "port": conf.get("port") or resolved.get("port", 27017),
                            "ssl_mode": conf.get("ssl_mode") or resolved.get("ssl_mode", "prefer"),
                        }
                    )
                else:
                    adapter = PostgresAdapter(
                        {
                            "dbname": conf.get("database") or resolved.get("database", ""),
                            "user": conf.get("username") or resolved.get("username", ""),
                            "password": conf.get("password") or resolved.get("password", ""),
                            "host": conf.get("host") or resolved.get("host", "localhost"),
                            "port": conf.get("port") or resolved.get("port", 5432),
                        }
                    )
                adapter.connect()
                adapter.close()
                return {"ok": True, "message": "connection successful"}
            except Exception as exc:
                return {"ok": False, "message": str(exc)}

        source = check(serializer.validated_data["source"])
        target = check(serializer.validated_data["target"])
        return Response({"ok": source["ok"] and target["ok"], "source": source, "target": target})


class MigrationPlanAPIView(APIView):
    def post(self, request):
        serializer = MigrationPlanSerializer(data=request.data)
        serializer.is_valid(raise_exception=True)
        data = serializer.validated_data

        source = ConnectionProfile.objects.get(pk=data["source_profile_id"])
        target = ConnectionProfile.objects.get(pk=data["target_profile_id"])
        job = MigrationJob.objects.create(
            name=data["name"],
            source_profile=source,
            target_profile=target,
            old_table=data["old_table"],
            new_table=data["new_table"],
            column_mapping=data["column_mapping"],
            batch_size=data["batch_size"],
            stop_on_error=data["stop_on_error"],
            status="PENDING",
        )
        return Response({"job_id": str(job.id), "status": job.status}, status=status.HTTP_201_CREATED)


class MigrationRunAPIView(APIView):
    def post(self, request, job_id):
        job = MigrationJob.objects.get(pk=job_id)
        execute(job)
        return Response({"job_id": str(job.id), "status": job.status}, status=status.HTTP_202_ACCEPTED)


class MigrationStatusAPIView(APIView):
    def get(self, request, job_id):
        job = MigrationJob.objects.get(pk=job_id)
        return Response(MigrationJobStatusSerializer(job).data)


class MigrationLogsAPIView(APIView):
    def get(self, request, job_id):
        job = MigrationJob.objects.get(pk=job_id)
        logs = job.logs.all().order_by("-ts")[:100]
        return Response({"job_id": str(job.id), "count": logs.count(), "results": MigrationLogSerializer(logs, many=True).data})
