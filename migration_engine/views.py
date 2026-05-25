from django.views.generic import TemplateView


class HeroView(TemplateView):
    template_name = "migration_engine/hero.html"


class DashboardView(TemplateView):
    template_name = "migration_engine/dashboard.html"


class LoginView(TemplateView):
    template_name = "migration_engine/login.html"


class NewMigrationView(TemplateView):
    template_name = "migration_engine/new_migration.html"


class JobDetailView(TemplateView):
    template_name = "migration_engine/job_detail.html"
