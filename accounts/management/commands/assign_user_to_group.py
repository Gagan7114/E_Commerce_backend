from django.contrib.auth.models import Group
from django.core.management.base import BaseCommand, CommandError

from accounts.models import User


class Command(BaseCommand):
    help = "Add a user (by email) to a group (by name)."

    def add_arguments(self, parser):
        parser.add_argument("email")
        parser.add_argument("group")

    def handle(self, *args, **options):
        email, group_name = options["email"], options["group"]
        try:
            user = User.objects.get(email=email)
        except User.DoesNotExist as exc:
            raise CommandError(f"No user with email {email!r}") from exc
        try:
            group = Group.objects.get(name=group_name)
        except Group.DoesNotExist as exc:
            raise CommandError(f"No group named {group_name!r}") from exc
        user.groups.add(group)
        self.stdout.write(self.style.SUCCESS(f"Added {email} to {group_name!r}."))
