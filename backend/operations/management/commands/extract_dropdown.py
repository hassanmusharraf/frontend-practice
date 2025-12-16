# Run: python manage.py runscript extract_dropdown
import json
from django.core.management.base import BaseCommand
from portal.models import DropDownValues  # Adjust this import

class Command(BaseCommand):
    help = 'Extract dropdown values from PostgreSQL'

    def handle(self, *args, **options):
        queryset = DropDownValues.objects.all().values(
            'dropdown_name', 'label', 'value'
        )

        data = list(queryset)
        with open('dropdown_values.json', 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, default=str)

        self.stdout.write(self.style.SUCCESS(f'Exported {len(data)} dropdown values to dropdown_values.json'))
