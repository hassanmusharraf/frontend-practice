# from django.utils import timezone
# from datetime import timedelta
# from django.db import transaction
# import logging
# from .models import (
#     ConsignmentStaging,
#     ConsignmentDocumentAttachmentStaging,
#     ConsignmentDocumentStaging,
#     PackagingAllocationStaging, 
#     ConsignmentPackagingStaging,
#     ConsignmentPOLineStaging,
#     PurchaseOrderUpload
# )
# from .task import process_purchase_orders

# logger = logging.getLogger("django_cron")

# # class StageConsignmentRemover:
#     # RUN_EVERY_MINS = 15
#     # schedule = Schedule(run_every_mins=RUN_EVERY_MINS)
#     # code = "my_unique_stage_consignment_remover_job"

# def process_consignments():

#     logger.info("Cron started: StageConsignmentRemover")
#     cutoff_time = timezone.now() - timedelta(minutes=15)

#     consignments = ConsignmentStaging.objects.filter(updated_at__lt=cutoff_time)
#     logger.info(f"Eligible consignments for deletion: {consignments.count()}")

#     for con in consignments:
#         logger.info(f"Deleting consignment ID: {con.id}")
#         try:
#             with transaction.atomic():
#                 ConsignmentDocumentAttachmentStaging.objects.filter(
#                     document__consignment=con
#                 ).delete()

#                 ConsignmentDocumentStaging.objects.filter(
#                     consignment=con
#                 ).delete()

#                 PackagingAllocationStaging.objects.filter(
#                     consignment_packaging__consignment=con
#                 ).delete()

#                 ConsignmentPackagingStaging.objects.filter(
#                     consignment=con
#                 ).delete()

#                 ConsignmentPOLineStaging.objects.filter(
#                     consignment=con
#                 ).delete()

#                 con.delete()
#                 logger.info(f"Deleted consignment ID: {con.id}")
#         except Exception as e:
#             logger.exception(f"Error deleting consignment ID {con.id}: {str(e)}")

#     logger.info("Cron completed: StageConsignmentRemover")

#     # self.process_purchase_orders()

# # def process_purchase_orders(self):

# #     logger.info("Cron started: ProcessPurchaseOrders")
# #     cutoff_time = timezone.now() - timedelta(minutes=30)

# #     objs = PurchaseOrderUpload.objects.filter(created_at__lt=cutoff_time)

# #     logger.info(f"Eligible consignments for processing: {objs.count()}")

# #     for obj in objs:
# #         try:

# #             process_purchase_orders.apply_async(args=[obj.id], countdown=30)
# #             logger.info(f"Processed purchase order ID: {obj.id}")

# #         except Exception as e:
# #             logger.exception(f"Error processing consignment ID {obj.id}: {str(e)}")

# #     logger.info("Cron completed: ProcessPurchaseOrders")