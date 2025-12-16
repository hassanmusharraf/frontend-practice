from django.apps import AppConfig
import json


class OperationsConfig(AppConfig):
    default_auto_field = 'django.db.models.BigAutoField'
    name = 'operations'


def setup_staging_cleanup_task():
    from django_celery_beat.models import PeriodicTask, IntervalSchedule
    schedule, created = IntervalSchedule.objects.get_or_create(
        every=5,
        period=IntervalSchedule.MINUTES,
    )

    PeriodicTask.objects.update_or_create(
        name='Delete old staging data',
        defaults={
            'interval': schedule,
            'task': 'operations.tasks.delete_old_staging_data',
            'args': json.dumps([]),
            'enabled': True,
        }
    )



from django.apps import AppConfig

class ConsignmentsConfig(AppConfig):
    default_auto_field = 'django.db.models.BigAutoField'
    name = 'consignments'

    def ready(self):
        from .task import delete_old_staging_data
        setup_staging_cleanup_task()
