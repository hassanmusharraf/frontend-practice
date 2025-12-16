import json
from django.db import models

class MSSQLJSONField(models.TextField):
    description = "Stores JSON as text in SQL Server"

    def from_db_value(self, value, expression, connection):
        """Convert DB string → Python dict/list automatically"""
        if value is None:
            return None
        try:
            return json.loads(value)
        except (ValueError, TypeError):
            return value  # if already plain text

    def to_python(self, value):
        """Ensure Python always gets a dict/list"""
        if isinstance(value, (dict, list)):
            return value
        if value is None:
            return None
        try:
            return json.loads(value)
        except (ValueError, TypeError):
            return value

    def get_prep_value(self, value):
        """Convert Python dict/list → JSON string before saving"""
        if value is None:
            return None
        return json.dumps(value)
