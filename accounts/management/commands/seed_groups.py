from django.contrib.auth.models import Group, Permission
from django.core.management.base import BaseCommand

from accounts.catalog import GROUP_CATALOG, PERMISSION_CATALOG


class Command(BaseCommand):
    help = "Seed the 7 default groups with permissions from the catalog."

    def handle(self, *args, **options):
        all_codes = [code for code, _ in PERMISSION_CATALOG]
        perms_by_code = {p.codename: p for p in Permission.objects.filter(codename__in=all_codes)}

        missing = [c for c in all_codes if c not in perms_by_code]
        if missing:
            self.stderr.write(self.style.ERROR(
                f"Missing permissions: {missing}. Run `seed_permissions` first."
            ))
            return

        for name, codes in GROUP_CATALOG.items():
            group, _ = Group.objects.get_or_create(name=name)
            if codes == ["*"]:
                resolved = list(perms_by_code.values())
            else:
                resolved = [perms_by_code[c] for c in codes if c in perms_by_code]
            group.permissions.set(resolved)
            self.stdout.write(f"  {name}: {len(resolved)} permissions")
        self.stdout.write(self.style.SUCCESS(f"Seeded {len(GROUP_CATALOG)} groups."))
