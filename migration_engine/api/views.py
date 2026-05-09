from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status
from migration_engine.application.workflow import execute
from migration_engine.domain.adapters.postgres import PostgresAdapter
from migration_engine.domain.adapters.sqlite import SQLiteAdapter
from migration_engine.models import ConnectionProfile, MigrationJob
from .serializers import ConnectionTestSerializer, MigrationPlanSerializer, MigrationJobStatusSerializer, MigrationLogSerializer


class ConnectionTestAPIView(APIView):
    def post(self, request):
        serializer = ConnectionTestSerializer(data=request.data)
        serializer.is_valid(raise_exception=True)

        def check(conf):
            try:
                if conf["db_type"] == "sqlite":
                    adapter = SQLiteAdapter(conf["database"])
                else:
                    adapter = PostgresAdapter(
                        {
                            "dbname": conf["database"],
                            "user": conf.get("username", ""),
                            "password": conf.get("password", ""),
                            "host": conf.get("host", "localhost"),
                            "port": conf.get("port", 5432),
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
