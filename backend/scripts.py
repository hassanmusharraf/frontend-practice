from django.test import TestCase

# Create your tests here.
from operations.models import ConsignmentPackaging, ConsignmentPackagingStaging, ConsignmentStaging, Consignment

print("Script started")

updated_count = 0
skipped_count = 0

for packaging in ConsignmentPackaging.objects.select_related("packaging_type"):
    # Find all staging records matching the same package_id and packaging_type
    stagings = ConsignmentPackagingStaging.objects.filter(
        package_id=packaging.package_id,
        packaging_type=packaging.packaging_type
    )

    if stagings.exists():
        for staging in stagings:
            staging.weight = packaging.packaging_type.weight
            staging.weight_unit = packaging.packaging_type.weight_unit
            staging.save()
            updated_count += 1
            print(f"✅ Updated: {staging.package_id}")
    else:
        skipped_count += 1
        print(f"⚠️ Skipped: No staging for package_id={packaging.package_id}")

print(f"\n🎯 Update complete. Total updated: {updated_count}, Skipped: {skipped_count}")