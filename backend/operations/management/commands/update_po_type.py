from django.core.management.base import BaseCommand

from operations.models import PurchaseOrder
from portal.choices import OrderTypeChoices

class Command(BaseCommand):
    def handle(self, *args, **kwargs):
        qs = PurchaseOrder.objects.select_related("storerkey").all()
        po_bto_type = qs.filter(storerkey__order_type=OrderTypeChoices.BTO)
        po_bts_type = qs.filter(storerkey__order_type=OrderTypeChoices.BTS)
        po_both_type = qs.filter(storerkey__order_type=OrderTypeChoices.BOTH)

        print(f"Updating {po_bto_type.count()} BTO type POs")
        po_bto_type.update(order_type=OrderTypeChoices.BTO)
        print(f"Updating {po_bts_type.count()} BTS type POs")
        po_bts_type.update(order_type=OrderTypeChoices.BTS)
        print(f"Updating {po_both_type.count()} BOTH type POs")
        po_both_type.update(order_type=OrderTypeChoices.BTO)  # default BOTH to BTO
        print("Update complete.")
