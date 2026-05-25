from django.views.generic import TemplateView


class DashboardView(TemplateView):
    template_name = "migration_engine/dashboard.html"


class NewMigrationView(TemplateView):
    template_name = "migration_engine/new_migration.html"


class JobDetailView(TemplateView):
    template_name = "migration_engine/job_detail.html"
