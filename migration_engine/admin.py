from django.contrib import admin
from .models import ConnectionProfile, MigrationJob, MigrationRunLog

admin.site.register(ConnectionProfile)
admin.site.register(MigrationJob)
admin.site.register(MigrationRunLog)
