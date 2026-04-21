from django.contrib.auth.models import Permission
from django.contrib.contenttypes.models import ContentType
from django.core.management.base import BaseCommand

from accounts.catalog import PERMISSION_CATALOG
from accounts.models import User


class Command(BaseCommand):
    help = "Seed or update the 25 ECMS custom permission codes."

    def handle(self, *args, **options):
        ct = ContentType.objects.get_for_model(User)
        created = updated = 0
        for codename, label in PERMISSION_CATALOG:
            obj, was_created = Permission.objects.update_or_create(
                codename=codename,
                content_type=ct,
                defaults={"name": label},
            )
            if was_created:
                created += 1
            else:
                updated += 1
        self.stdout.write(self.style.SUCCESS(
            f"Permissions seeded — {created} created, {updated} updated."
        ))
