from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("migration_engine", "0002_connectionprofile_uri_and_mongodb_choice"),
    ]

    operations = [
        migrations.AddField(
            model_name="connectionprofile",
            name="hosted_api_token",
            field=models.CharField(blank=True, default="", max_length=255),
        ),
        migrations.AddField(
            model_name="connectionprofile",
            name="hosted_api_url",
            field=models.URLField(blank=True, default=""),
        ),
        migrations.AddField(
            model_name="connectionprofile",
            name="hosted_metadata",
            field=models.JSONField(blank=True, default=dict),
        ),
        migrations.AddField(
            model_name="connectionprofile",
            name="hosted_provider",
            field=models.CharField(blank=True, choices=[("", "None"), ("render", "Render"), ("vercel", "Vercel"), ("railway", "Railway"), ("netlify", "Netlify")], default="", max_length=20),
        ),
        migrations.AddField(
            model_name="connectionprofile",
            name="hosted_resource_id",
            field=models.CharField(blank=True, default="", max_length=255),
        ),
    ]
