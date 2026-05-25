from django.urls import path
from .views import (
    ConnectionProfileAPIView,
    ConnectionTestAPIView,
    MigrationJobsAPIView,
    MigrationPlanAPIView,
    MigrationRunAPIView,
    MigrationStatusAPIView,
    MigrationLogsAPIView,
)

urlpatterns = [
    path("migrations/profiles", ConnectionProfileAPIView.as_view()),
    path("migrations/connections/test", ConnectionTestAPIView.as_view()),
    path("migrations/jobs", MigrationJobsAPIView.as_view()),
    path("migrations/plan", MigrationPlanAPIView.as_view()),
    path("migrations/<int:job_id>/run", MigrationRunAPIView.as_view()),
    path("migrations/<int:job_id>/status", MigrationStatusAPIView.as_view()),
    path("migrations/<int:job_id>/logs", MigrationLogsAPIView.as_view()),
]
