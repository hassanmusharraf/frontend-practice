from django.core.management.base import BaseCommand
from django.utils import timezone
from datetime import timedelta
from operations.task import process_purchase_orders_v2
from operations.models import (
    Consignment,
    PurchaseOrderUpload,ConsignmentDocumentAttachment, ComprehensiveReport
)
from portal.choices import POUploadStatusChoices, ConsignmentStatusChoices, POImportFormatsChoices
from operations.utils import update_files
from operations.task import generate_comperhensive_report
from operations.other_services.po_import import SLBPOImportService
from core.response import ServiceError
from django.db.models import Q
from operations.notifications import NotificationService

class Command(BaseCommand):
    help = 'A simple test command to log the current time'

    def handle(self, *args, **kwargs):

        ## Delete 10min older draft consignments and their associated documents and attachments
        self.stdout.write(self.style.SUCCESS(f"Script started"))

        consignments = Consignment.objects.filter(consignment_status=ConsignmentStatusChoices.DRAFT, updated_at__lt=timezone.now() - timedelta(minutes=45)).values_list("id", flat=True)    
        self.stdout.write(self.style.SUCCESS(f"Found {len(consignments)} consignments to delete."))
        attachments = ConsignmentDocumentAttachment.objects.filter(document__consignment_id__in=consignments).values_list("id", flat=True)
        self.stdout.write(self.style.SUCCESS(f"Found {len(attachments)} attachments to delete."))
        update_files(deleted_doc_ids=attachments)
        if attachments:
            ConsignmentDocumentAttachment.objects.filter(id__in=attachments).delete()
        if consignments:
            Consignment.objects.filter(id__in=consignments).delete()

        self.stdout.write(self.style.SUCCESS(f"Deleted attachments"))










        self.stdout.write(self.style.SUCCESS("Cron started: ProcessPurchaseOrders"))
        

        objs = PurchaseOrderUpload.objects.filter(created_at__lt=timezone.now(),status = POUploadStatusChoices.IN_PROGRESS)
        self.stdout.write(self.style.SUCCESS(f"Eligible PO files for processing: {objs.count()}"))
        for obj in objs:
            try:
                obj.status = POUploadStatusChoices.QUEUE
                obj.save()
                if obj.file_format == POImportFormatsChoices.SLB:
                    self.stdout.write(self.style.SUCCESS(f"Processing SLB File: {obj.id}"))
                    SLBPOImportService.process_slb_po_file(obj)
                else:
                    # process_purchase_orders(obj.id)
                    self.stdout.write(self.style.SUCCESS(f"Processing File: {obj.id}"))
                    process_purchase_orders_v2(obj.id)
                self.stdout.write(self.style.SUCCESS(f"Processed purchase order ID: {obj.id}"))

            except ServiceError as e:
                self.stdout.write(self.style.ERROR(f"Error processing file {obj.id}: {str(e)}"))
                obj.status = POUploadStatusChoices.FAILED
                obj.save()

            except Exception as e:
                self.stdout.write(self.style.ERROR(f"Error processing file {obj.id}: {str(e)}"))
                obj.status = POUploadStatusChoices.FAILED
                obj.save()


        self.stdout.write(self.style.SUCCESS("Cron completed: ProcessPurchaseOrders"))










        try:
            self.stdout.write(self.style.SUCCESS("GenerateComprehensiveReport Generation Started...."))
            
            # qs = ComprehensiveReport.objects.values("from_date", "to_date", "status","consignment_ids").order_by("-created_at").first()
            # ComprehensiveReport.objects.filter(report_generation_status=POUploadStatusChoices.QUEUE).update(report_generation_status=POUploadStatusChoices.SUCCESS)
            instance = ComprehensiveReport.objects.filter(report_generation_status=POUploadStatusChoices.QUEUE).order_by("-created_at").first()

            if not instance:
                self.stdout.write(self.style.ERROR("No ComprehensiveReport instance found. Exiting."))
                return
            
            # instance.report_generation_status = POUploadStatusChoices.QUEUE
            # instance.save(update_fields=["report_generation_status"])

            qs = {
                "from_date": instance.from_date,
                "to_date": instance.to_date,
                "status": instance.status,
                "consignment_ids": instance.consignment_ids,
            }

            filters = Q(created_at__gte=instance.from_date, created_at__lte=instance.to_date)

            if instance.status and instance.status != "all":
                filters &= Q(consignment_status__in=instance.status)

            if instance.consignment_ids:
                filters &= Q(consignment_id__in=instance.consignment_ids)

            # filters = Q(created_at__gte=qs["from_date"], created_at__lte=qs["to_date"])

            # if qs["status"] and qs["status"] != 'all':
            #     filters &= Q(consignment_status__in=qs["status"])

            # if qs["consignment_ids"]:
            #     filters &= Q(consignment_id__in=qs["consignment_ids"])


            consignments_qs = Consignment.objects.filter(filters).exclude(
                consignment_status=ConsignmentStatusChoices.DRAFT
            )
            
            generate_comperhensive_report(consignments_qs)

            instance.report_generation_status = POUploadStatusChoices.SUCCESS
            instance.save(update_fields=["report_generation_status"])

            NotificationService.notify_comprehensive_report(
                user=instance.user,
                header="Comprehensive Report",
                message="Comprehensive Report generation is completed.",
                attachment = str(ConsignmentDocumentAttachment.objects.filter(document__document_type="Comprehensive Report").last().file.url) if ConsignmentDocumentAttachment.objects.filter(document__document_type="Comprehensive Report").exists() else None
            )


            self.stdout.write(self.style.SUCCESS("GenerateComprehensiveReport Generation Completed."))

        except Exception as e:
            self.stdout.write(self.style.ERROR(f"Error in ComprehensiveReport Generation: {str(e)}"))
            if instance:
                instance.report_generation_status = POUploadStatusChoices.FAILED
                instance.save(update_fields=["report_generation_status"])

