from django.db import models


class ConnectionProfile(models.Model):
    DB_TYPES = (("sqlite", "SQLite"), ("postgres", "PostgreSQL"))
    name = models.CharField(max_length=120, unique=True)
    db_type = models.CharField(max_length=20, choices=DB_TYPES)
    database = models.CharField(max_length=255)
    host = models.CharField(max_length=255, blank=True, default="")
    port = models.IntegerField(null=True, blank=True)
    username = models.CharField(max_length=255, blank=True, default="")
    password = models.CharField(max_length=255, blank=True, default="")
    ssl_mode = models.CharField(max_length=50, blank=True, default="prefer")
    created_at = models.DateTimeField(auto_now_add=True)


class MigrationJob(models.Model):
    STATUS_CHOICES = [
        ("PENDING", "PENDING"), ("PLANNING", "PLANNING"), ("PROFILING", "PROFILING"),
        ("MAPPING", "MAPPING"), ("TRANSFORMING", "TRANSFORMING"), ("LOADING", "LOADING"),
        ("VERIFYING", "VERIFYING"), ("SUCCESS", "SUCCESS"), ("FAILED", "FAILED")
    ]
    name = models.CharField(max_length=150)
    source_profile = models.ForeignKey(ConnectionProfile, on_delete=models.CASCADE, related_name="source_jobs")
    target_profile = models.ForeignKey(ConnectionProfile, on_delete=models.CASCADE, related_name="target_jobs")
    old_table = models.CharField(max_length=120)
    new_table = models.CharField(max_length=120)
    column_mapping = models.JSONField(default=dict)
    batch_size = models.IntegerField(default=500)
    stop_on_error = models.BooleanField(default=True)
    status = models.CharField(max_length=20, choices=STATUS_CHOICES, default="PENDING")
    stage = models.CharField(max_length=30, blank=True, default="")
    rows_read = models.IntegerField(default=0)
    rows_written = models.IntegerField(default=0)
    errors = models.IntegerField(default=0)
    created_at = models.DateTimeField(auto_now_add=True)
    started_at = models.DateTimeField(null=True, blank=True)
    finished_at = models.DateTimeField(null=True, blank=True)
    updated_at = models.DateTimeField(auto_now=True)


class MigrationRunLog(models.Model):
    LEVELS = (("INFO", "INFO"), ("WARNING", "WARNING"), ("ERROR", "ERROR"))
    job = models.ForeignKey(MigrationJob, on_delete=models.CASCADE, related_name="logs")
    stage = models.CharField(max_length=50)
    level = models.CharField(max_length=20, choices=LEVELS, default="INFO")
    message = models.TextField()
    ts = models.DateTimeField(auto_now_add=True)
