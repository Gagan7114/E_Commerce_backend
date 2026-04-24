"""Periodic refresh job for the Monthly Targets feature.

Recomputes the derived columns (done_ltrs, done_value, date, est_ltr,
est_value, achieved_pct, est_ltr_pct, growth, growth_pct) on every
month_targets row in the CURRENT calendar month. Never touches
`targets` or `last_month`, and never touches rows in closed months.

Run via cron / Task Scheduler, e.g. hourly:
    python manage.py refresh_monthly_targets

Optional flags:
    --month M --year Y    Force a specific month (must still be current).
    --dry-run             Show what would change without writing.
"""

from __future__ import annotations

from datetime import date
from decimal import Decimal

from django.core.management.base import BaseCommand, CommandError
from django.db import connection

from platforms.models import PlatformConfig
from platforms.monthly_targets import (
    IN_SCOPE_SLUGS,
    _compute_derived,
    _format_for,
    _is_current_month,
    _read_source,
)


class Command(BaseCommand):
    help = "Refresh derived columns on every current-month month_targets row."

    def add_arguments(self, parser):
        parser.add_argument("--month", type=int, default=None,
                            help="Month 1-12 (defaults to current month).")
        parser.add_argument("--year", type=int, default=None,
                            help="Year (defaults to current year).")
        parser.add_argument("--dry-run", action="store_true",
                            help="Print intended updates without writing.")

    def handle(self, *args, **opts):
        today = date.today()
        month = opts["month"] or today.month
        year = opts["year"] or today.year
        dry = opts["dry_run"]

        if not _is_current_month(month, year, today):
            raise CommandError(
                f"Refusing to refresh {month:02d}-{year}: only the current "
                f"calendar month ({today.month:02d}-{today.year}) may be refreshed."
            )

        # Map slug → PlatformConfig for the format lookup in in-scope slugs.
        platforms = {
            p.slug: p for p in PlatformConfig.objects.filter(slug__in=IN_SCOPE_SLUGS)
        }

        with connection.cursor() as cur:
            cur.execute(
                """
                SELECT id, "format", item_head, targets, last_month
                  FROM month_targets
                 WHERE month = %s AND year = %s
                """,
                [month, year],
            )
            rows = cur.fetchall()

        if not rows:
            self.stdout.write(f"No rows found for {month:02d}-{year}. Nothing to do.")
            return

        updated = 0
        skipped = 0
        for row_id, fmt, item_head, targets, last_month in rows:
            slug = self._slug_for_format(fmt, platforms)
            if slug is None:
                self.stdout.write(
                    self.style.WARNING(
                        f"  id={row_id} format={fmt!r}: no in-scope platform "
                        "matches; skipping."
                    )
                )
                skipped += 1
                continue

            source = _read_source(slug, fmt, item_head, month, year)
            derived = _compute_derived(
                targets=Decimal(targets or 0),
                done_ltrs=source["done_ltrs"],
                done_value=source["done_value"],
                latest_date=source["latest_date"],
                last_month=Decimal(last_month or 0),
                month=month,
                year=year,
            )

            if dry:
                self.stdout.write(
                    f"  id={row_id} {slug}/{item_head}: "
                    f"done_ltrs={derived['done_ltrs']} "
                    f"est_ltr={derived['est_ltr']} "
                    f"(dry-run)"
                )
                updated += 1
                continue

            with connection.cursor() as cur:
                cur.execute(
                    """
                    UPDATE month_targets
                       SET "date"       = %s,
                           done_ltrs    = %s,
                           done_value   = %s,
                           achieved_pct = %s,
                           est_ltr      = %s,
                           est_value    = %s,
                           est_ltr_pct  = %s,
                           growth       = %s,
                           growth_pct   = %s,
                           updated_at   = NOW()
                     WHERE id = %s
                    """,
                    [
                        derived["date"],
                        derived["done_ltrs"], derived["done_value"],
                        derived["achieved_pct"],
                        derived["est_ltr"], derived["est_value"],
                        derived["est_ltr_pct"],
                        derived["growth"], derived["growth_pct"],
                        row_id,
                    ],
                )
            updated += 1

        verb = "would update" if dry else "updated"
        self.stdout.write(self.style.SUCCESS(
            f"{verb} {updated} row(s); skipped {skipped}."
        ))

    @staticmethod
    def _slug_for_format(fmt: str, platforms: dict[str, PlatformConfig]) -> str | None:
        if fmt is None:
            return None
        target = fmt.strip().lower()
        for slug, p in platforms.items():
            if _format_for(p).strip().lower() == target:
                return slug
        return None
