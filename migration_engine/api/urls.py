from django.urls import path
from .views import (
    ConnectionTestAPIView,
    MigrationPlanAPIView,
    MigrationRunAPIView,
    MigrationStatusAPIView,
    MigrationLogsAPIView,
)

urlpatterns = [
    path("migrations/connections/test", ConnectionTestAPIView.as_view()),
    path("migrations/plan", MigrationPlanAPIView.as_view()),
    path("migrations/<int:job_id>/run", MigrationRunAPIView.as_view()),
    path("migrations/<int:job_id>/status", MigrationStatusAPIView.as_view()),
    path("migrations/<int:job_id>/logs", MigrationLogsAPIView.as_view()),
]
