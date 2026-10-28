from django.contrib import admin
from django.urls import path, include
from migration_engine.views import HeroView, DashboardView, LoginView, NewMigrationView, JobDetailView

urlpatterns = [
    path('admin/', admin.site.urls),
    path('api/v1/', include('migration_engine.api.urls')),
    path('dj-rest-auth/', include('dj_rest_auth.urls')),

    path('', HeroView.as_view()),
    path('login/', LoginView.as_view()),
    path('dashboard/', DashboardView.as_view()),
    path('migrations/new/', NewMigrationView.as_view()),
    path('migrations/<int:job_id>/', JobDetailView.as_view()),
]
