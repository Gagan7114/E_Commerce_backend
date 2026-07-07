"""Report feed freshness and exit non-zero if any feed is stale.

Read-only (one MAX() per feed — see dashboard.feed_health). Meant for cron:

    python manage.py check_feed_freshness            # human table
    python manage.py check_feed_freshness --quiet    # only print stale/no_data

A non-zero exit lets a wrapper pipe the output to Telegram/Slack only when
something is actually wrong — the permanent guard against silently-dead feeds
(audit #8/#10). This never restarts a feed; it just makes the death loud.
"""
from django.core.management.base import BaseCommand

from dashboard import feed_health


class Command(BaseCommand):
    help = "Print feed freshness; exit 1 if any feed is stale or has no data."

    def add_arguments(self, parser):
        parser.add_argument(
            "--quiet", action="store_true",
            help="Only print stale / no-data feeds (nothing when all fresh).",
        )

    def handle(self, *args, **options):
        rows = feed_health.feed_freshness()
        bad = [r for r in rows if r["status"] != "fresh"]
        show = bad if options["quiet"] else rows

        for r in show:
            age = "n/a" if r["age_days"] is None else f"{r['age_days']}d"
            line = f"[{r['status'].upper():7}] {r['feed']:22} last={r['max_date'] or 'never':10} age={age} (limit {r['threshold_days']}d)"
            if r["status"] == "fresh":
                self.stdout.write(self.style.SUCCESS(line))
            elif r["status"] == "stale":
                self.stdout.write(self.style.WARNING(line))
            else:
                self.stdout.write(self.style.ERROR(line))

        if bad:
            names = ", ".join(r["feed"] for r in bad)
            self.stderr.write(self.style.ERROR(f"\n{len(bad)} feed(s) need attention: {names}"))
            raise SystemExit(1)
        self.stdout.write(self.style.SUCCESS("\nAll feeds fresh."))
