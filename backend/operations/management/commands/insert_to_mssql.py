# Run: python manage.py runscript insert_from_json
import json
from django.core.management.base import BaseCommand
from portal.models import DropDownValues  # replace with your app/model

class Command(BaseCommand):
    help = 'Insert dropdown values from dropdown_values.json into the database'

    def handle(self, *args, **options):
        with open('dropdown_values.json', 'r', encoding='utf-8') as f:
            data = json.load(f)

        count = 0
        for item in data:
            print(item)
            DropDownValues.objects.create(
                dropdown_name=item['dropdown_name'],
                label=item['label'],
                value=item['value'],
                is_active=True
            )
            count += 1

        self.stdout.write(self.style.SUCCESS(f'Inserted {count} records from dropdown_values.json'))
