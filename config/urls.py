from django.contrib import admin
from django.urls import path, include
from migration_engine.views import DashboardView, NewMigrationView, JobDetailView

urlpatterns = [
    path('admin/', admin.site.urls),
    path('api/v1/', include('migration_engine.api.urls')),
    path('', DashboardView.as_view()),
    path('migrations/new/', NewMigrationView.as_view()),
    path('migrations/<int:job_id>/', JobDetailView.as_view()),
]
