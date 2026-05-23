from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("migration_engine", "0001_initial"),
    ]

    operations = [
        migrations.AddField(
            model_name="connectionprofile",
            name="uri",
            field=models.TextField(blank=True, default=""),
        ),
        migrations.AlterField(
            model_name="connectionprofile",
            name="db_type",
            field=models.CharField(
                choices=[("sqlite", "SQLite"), ("postgres", "PostgreSQL"), ("mongodb", "MongoDB")],
                max_length=20,
            ),
        ),
    ]
