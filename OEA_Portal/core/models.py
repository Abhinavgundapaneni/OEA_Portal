import uuid
from django.db import models

class InstallationLogs(models.Model):
    id = models.UUIDField(default=uuid.uuid4,
                          max_length=50,
                          primary_key=True)
    request_id = models.CharField(max_length=20)
    action = models.CharField(max_length=20)
    message = models.TextField(max_length=500)
    timestamp = models.DateTimeField(auto_now=True)

class TableMetadata(models.Model):
    id = models.IntegerField(auto_created=True, primary_key=True)
    table_name = models.CharField(max_length=100)
    column_name = models.CharField(max_length=100)
    column_type = models.CharField(max_length=20)
    constraint = models.CharField(max_length=20)
    pseuodynimization = models.CharField(max_length=20)