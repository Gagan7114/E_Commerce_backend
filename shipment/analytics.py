"""Planner dashboard analytics.

One endpoint feeding every panel on the shipment-planner dashboard. The three
questions a planner actually holds in their head at once — what is BOOKED
(appointments), what is PLANNED (shipments), what is still OUTSTANDING
(pendency) — used to live on three separate pages. Serving them from one call
means the page makes one request and no panel can disagree with its neighbour
about the filter it was drawn under.

Kept out of views.py deliberately: that module is already six thousand lines,
and none of this is needed by any other view.
"""

from datetime import datetime as _datetime, timedelta

from django.db import connection
from django.utils import timezone
from rest_framework.permissions import IsAuthenticated
from rest_framework.response import Response

from .views import _SafeAPIView, MIN_DAYS_TO_EXPIRY

# A dashboard is a glance, not an archive: a year bounds the day-series and
# keeps the page honest about what it can draw.
MAX_RANGE_DAYS = 366
DEFAULT_RANGE_DAYS = 30
# Sentinel FC for "the filters matched nothing". An empty list would be read as
# "no filter" and quietly show everything, which is the opposite of what was
# asked. It must be a value no real FC can take AND a legal Postgres text value
# — NUL bytes are not (text fields reject 0x00), which turned this fail-closed
# path into a 500 the first time a disjoint channel + FC pair was selected.
_NO_MATCH = '__NO_MATCHING_FC__'


def _parse_date(raw, fallback):
    try:
        return _datetime.fromisoformat(str(raw)[:10]).date()
    except (TypeError, ValueError):
        return fallback


def _csv_upper(raw):
    return [v.strip().upper() for v in str(raw or '').split(',') if v.strip()]


class ShipmentAnalyticsView(_SafeAPIView):
    """GET /api/shipment/analytics/

    Params
      from, to   ISO dates. Default: the last 30 days ending today.
      fc         CSV of fulfillment centers.
      channel    CSV of CORE / FRESH / NOW, resolved to FCs through
                 fc_city_state_channel_master and intersected with `fc`.

    Booked and planned figures are windowed by the date range. Pendency is a
    snapshot of what is open RIGHT NOW — an outstanding PO is outstanding today
    whichever fortnight you are looking at — so it answers the FC and channel
    filters but not the dates. That is stated in the payload and labelled in the
    UI, because a figure that silently ignores half the filter is worse than one
    that explains itself.

    Pendency reuses the SKU Pendency page's own predicates rather than
    re-deriving them, so the dashboard and that page can never drift apart.
    """

    permission_classes = [IsAuthenticated]

    def get(self, request):
        # Imported here, not at module scope: uploads.amazon_uploads is a heavy
        # module and this keeps it off the import path of every other view.
        from uploads.amazon_uploads import (
            _SKU_PENDENCY_PENDING,
            _SKU_PENDENCY_HAS_STATED_LITRE,
        )

        today = timezone.localdate()
        d_to = _parse_date(request.query_params.get('to'), today)
        d_from = _parse_date(
            request.query_params.get('from'), d_to - timedelta(days=DEFAULT_RANGE_DAYS - 1)
        )
        if d_from > d_to:
            d_from, d_to = d_to, d_from
        if (d_to - d_from).days > MAX_RANGE_DAYS:
            d_from = d_to - timedelta(days=MAX_RANGE_DAYS)

        want_fcs = _csv_upper(request.query_params.get('fc'))
        want_channels = _csv_upper(request.query_params.get('channel'))

        with connection.cursor() as cur:
            cur.execute(
                "SELECT UPPER(TRIM(fc)), UPPER(TRIM(COALESCE(channel, ''))) "
                "FROM fc_city_state_channel_master WHERE COALESCE(TRIM(fc), '') <> ''"
            )
            fc_channel = {fc: ch for fc, ch in cur.fetchall()}

        fcs = set(want_fcs)
        if want_channels:
            in_channel = {fc for fc, ch in fc_channel.items() if ch in want_channels}
            fcs = (fcs & in_channel) if fcs else in_channel
            if not fcs:
                fcs = {_NO_MATCH}
        fc_list = sorted(fcs) if fcs else None

        def fc_clause(column):
            """(sql, params) restricting `column` to the effective FC list."""
            if not fc_list:
                return '', []
            return f' AND UPPER(TRIM({column})) = ANY(%s::text[])', [fc_list]

        out = {
            'range': {
                'from': d_from.isoformat(),
                'to': d_to.isoformat(),
                'days': (d_to - d_from).days + 1,
            },
            'filters': {
                'fc': sorted(want_fcs),
                'channel': sorted(want_channels),
                'fcs_applied': [f for f in (fc_list or []) if f != _NO_MATCH],
            },
        }

        with connection.cursor() as cur:
            # ── Booked ──────────────────────────────────────────────────────
            # COUNT(DISTINCT appointment_id), never COUNT(*):
            # reporting."appointment" carries one row per PO line, so a
            # three-PO appointment would otherwise be counted three times.
            where, params = fc_clause('a.destination_fc')
            cur.execute(
                f"""
                SELECT DATE(a.appointment_time) AS d,
                       COUNT(DISTINCT a.appointment_id) FILTER (WHERE a.status = 'Confirmed') AS confirmed,
                       COUNT(DISTINCT a.appointment_id) FILTER (WHERE a.status = 'Cancelled') AS cancelled,
                       COUNT(DISTINCT a.appointment_id) FILTER (WHERE a.status = 'Closed')    AS closed,
                       COUNT(DISTINCT a.appointment_id) AS total
                  FROM reporting."appointment" a
                 WHERE DATE(a.appointment_time) BETWEEN %s AND %s{where}
                 GROUP BY 1 ORDER BY 1
                """,
                [d_from, d_to] + params,
            )
            out['appointments_by_day'] = [
                {'date': d.isoformat(), 'confirmed': c, 'cancelled': x, 'closed': cl, 'total': t}
                for d, c, x, cl, t in cur.fetchall()
            ]

            cur.execute(
                f"""
                SELECT UPPER(TRIM(COALESCE(a.destination_fc, ''))) AS fc,
                       COUNT(DISTINCT a.appointment_id) AS total,
                       COUNT(DISTINCT a.appointment_id) FILTER (WHERE a.status = 'Confirmed') AS confirmed,
                       COUNT(DISTINCT a.appointment_id) FILTER (WHERE a.status = 'Cancelled') AS cancelled
                  FROM reporting."appointment" a
                 WHERE DATE(a.appointment_time) BETWEEN %s AND %s{where}
                 GROUP BY 1
                HAVING UPPER(TRIM(COALESCE(a.destination_fc, ''))) <> ''
                 ORDER BY 2 DESC
                """,
                [d_from, d_to] + params,
            )
            out['appointments_by_fc'] = [
                {'fc': fc, 'channel': fc_channel.get(fc, ''),
                 'total': t, 'confirmed': c, 'cancelled': x}
                for fc, t, c, x in cur.fetchall()
            ]

            cur.execute(
                f"""
                SELECT COALESCE(NULLIF(TRIM(a.status), ''), 'Unknown') AS status,
                       COUNT(DISTINCT a.appointment_id) AS n
                  FROM reporting."appointment" a
                 WHERE DATE(a.appointment_time) BETWEEN %s AND %s{where}
                 GROUP BY 1 ORDER BY 2 DESC
                """,
                [d_from, d_to] + params,
            )
            out['appointment_status_mix'] = [
                {'status': s, 'count': n} for s, n in cur.fetchall()
            ]

            # ── Planned ─────────────────────────────────────────────────────
            where, params = fc_clause('s.destination_fc')
            cur.execute(
                f"""
                SELECT DATE(s.created_at) AS d,
                       COUNT(*) AS shipments,
                       COALESCE(SUM(s.planned_liters), 0) AS liters,
                       COALESCE(AVG(NULLIF(s.load_percentage, 0)), 0) AS avg_fill
                  FROM sp_shipments s
                 WHERE DATE(s.created_at) BETWEEN %s AND %s{where}
                 GROUP BY 1 ORDER BY 1
                """,
                [d_from, d_to] + params,
            )
            out['shipments_by_day'] = [
                {'date': d.isoformat(), 'shipments': n,
                 'liters': float(l or 0), 'avg_fill': float(f or 0)}
                for d, n, l, f in cur.fetchall()
            ]

            cur.execute(
                f"""
                SELECT s.status, COUNT(*) AS n
                  FROM sp_shipments s
                 WHERE DATE(s.created_at) BETWEEN %s AND %s{where}
                 GROUP BY 1 ORDER BY 2 DESC
                """,
                [d_from, d_to] + params,
            )
            out['shipment_status_mix'] = [
                {'status': s, 'count': n} for s, n in cur.fetchall()
            ]

            # Fill in buckets, not as one average: a 30%-full truck and a
            # 90%-full one average to a healthy-looking 60% that never left the
            # yard. The shape is the point.
            cur.execute(
                f"""
                SELECT width_bucket(
                           LEAST(GREATEST(COALESCE(s.load_percentage, 0), 0), 99.999), 0, 100, 5) AS b,
                       COUNT(*) AS n
                  FROM sp_shipments s
                 WHERE DATE(s.created_at) BETWEEN %s AND %s{where}
                 GROUP BY 1 ORDER BY 1
                """,
                [d_from, d_to] + params,
            )
            buckets = {int(b): n for b, n in cur.fetchall() if b}
            out['fill_buckets'] = [
                {'label': f'{i * 20}-{(i + 1) * 20}%', 'count': buckets.get(i + 1, 0)}
                for i in range(5)
            ]

            where_i, params_i = fc_clause('i.destination_fc')
            cur.execute(
                f"""
                SELECT UPPER(TRIM(COALESCE(i.destination_fc, ''))) AS fc,
                       COALESCE(SUM(i.planned_qty), 0) AS units,
                       COALESCE(SUM(i.planned_liters), 0) AS liters,
                       COUNT(DISTINCT i.po_number) AS pos
                  FROM sp_items i
                  JOIN sp_shipments s ON s.id = i.shipment_id
                 WHERE i.not_loaded = FALSE
                   AND DATE(s.created_at) BETWEEN %s AND %s{where_i}
                 GROUP BY 1
                HAVING UPPER(TRIM(COALESCE(i.destination_fc, ''))) <> ''
                 ORDER BY 3 DESC
                """,
                [d_from, d_to] + params_i,
            )
            out['volume_by_fc'] = [
                {'fc': fc, 'channel': fc_channel.get(fc, ''),
                 'units': float(u or 0), 'liters': float(l or 0), 'pos': p}
                for fc, u, l, p in cur.fetchall()
            ]

            # Premium / Commodity / Other — the same bucketing itemHead.js uses,
            # so the dashboard splits volume exactly as the PDF and detail page do.
            cur.execute(
                f"""
                SELECT CASE
                         WHEN POSITION('premium' IN LOWER(COALESCE(i.item_head, ''))) > 0
                           THEN 'Premium'
                         WHEN POSITION('commodity' IN LOWER(COALESCE(i.item_head, ''))) > 0
                           THEN 'Commodity'
                         ELSE 'Other' END AS head,
                       COALESCE(SUM(i.planned_liters), 0) AS liters,
                       COALESCE(SUM(i.planned_qty), 0) AS units
                  FROM sp_items i
                  JOIN sp_shipments s ON s.id = i.shipment_id
                 WHERE i.not_loaded = FALSE
                   AND DATE(s.created_at) BETWEEN %s AND %s{where_i}
                 GROUP BY 1 ORDER BY 2 DESC
                """,
                [d_from, d_to] + params_i,
            )
            out['category_mix'] = [
                {'head': h, 'liters': float(l or 0), 'units': float(u or 0)}
                for h, l, u in cur.fetchall()
            ]

            # ── Outstanding (a NOW snapshot, not date-windowed) ──────────────
            litres = (
                f'CASE WHEN {_SKU_PENDENCY_HAS_STATED_LITRE} '
                f'THEN reporting."Amazon PO"."remaining_ltrs" ELSE 0 END'
            )
            where_p, params_p = fc_clause('reporting."Amazon PO".fulfillment_center')

            cur.execute(
                f"""
                SELECT UPPER(TRIM(COALESCE(fulfillment_center, ''))) AS fc,
                       COUNT(DISTINCT po_number) AS pos,
                       COUNT(*) AS lines,
                       COALESCE(SUM(COALESCE(remaining_qty, 0)), 0) AS units,
                       COALESCE(SUM({litres}), 0) AS liters
                  FROM reporting."Amazon PO"
                 WHERE {_SKU_PENDENCY_PENDING}{where_p}
                 GROUP BY 1
                HAVING UPPER(TRIM(COALESCE(fulfillment_center, ''))) <> ''
                 ORDER BY 5 DESC
                """,
                params_p,
            )
            out['pendency_by_fc'] = [
                {'fc': fc, 'channel': fc_channel.get(fc, ''), 'pos': p, 'lines': ln,
                 'units': float(u or 0), 'liters': float(l or 0)}
                for fc, p, ln, u, l in cur.fetchall()
            ]

            cur.execute(
                f"""
                SELECT UPPER(TRIM(COALESCE(asin, ''))) AS asin,
                       MAX(sku_name) AS sku,
                       COALESCE(SUM(COALESCE(remaining_qty, 0)), 0) AS units,
                       COALESCE(SUM({litres}), 0) AS liters,
                       COUNT(DISTINCT po_number) AS pos
                  FROM reporting."Amazon PO"
                 WHERE {_SKU_PENDENCY_PENDING}{where_p}
                 GROUP BY 1
                HAVING UPPER(TRIM(COALESCE(asin, ''))) <> ''
                 ORDER BY 4 DESC, 3 DESC
                 LIMIT 12
                """,
                params_p,
            )
            out['top_pending_skus'] = [
                {'asin': a, 'sku': s or a, 'units': float(u or 0),
                 'liters': float(l or 0), 'pos': p}
                for a, s, u, l, p in cur.fetchall()
            ]

            # Ageing. A pendency total says how much is owed; this says how long
            # it has been owed, which is the half that decides what loads next.
            cur.execute(
                f"""
                SELECT CASE
                         WHEN order_date IS NULL THEN 'Unknown'
                         WHEN CURRENT_DATE - order_date::date <= 7  THEN '0-7 days'
                         WHEN CURRENT_DATE - order_date::date <= 14 THEN '8-14 days'
                         WHEN CURRENT_DATE - order_date::date <= 30 THEN '15-30 days'
                         ELSE '30+ days' END AS bucket,
                       COUNT(DISTINCT po_number) AS pos,
                       COALESCE(SUM(COALESCE(remaining_qty, 0)), 0) AS units,
                       COALESCE(SUM({litres}), 0) AS liters
                  FROM reporting."Amazon PO"
                 WHERE {_SKU_PENDENCY_PENDING}{where_p}
                 GROUP BY 1
                """,
                params_p,
            )
            aged = {
                b: {'bucket': b, 'pos': p, 'units': float(u or 0), 'liters': float(l or 0)}
                for b, p, u, l in cur.fetchall()
            }
            out['pendency_ageing'] = [
                aged.get(b, {'bucket': b, 'pos': 0, 'units': 0.0, 'liters': 0.0})
                for b in ('0-7 days', '8-14 days', '15-30 days', '30+ days', 'Unknown')
            ]

            # POs about to cancel themselves. Same cutoff the planner enforces,
            # so the dashboard warns about exactly what the planner will refuse.
            cur.execute(
                f"""
                SELECT COUNT(DISTINCT po_number) AS pos,
                       COALESCE(SUM(COALESCE(remaining_qty, 0)), 0) AS units,
                       COALESCE(SUM({litres}), 0) AS liters
                  FROM reporting."Amazon PO"
                 WHERE {_SKU_PENDENCY_PENDING}{where_p}
                   AND expiry_date IS NOT NULL
                   AND expiry_date::date - CURRENT_DATE BETWEEN 0 AND %s
                """,
                params_p + [MIN_DAYS_TO_EXPIRY],
            )
            row = cur.fetchone() or (0, 0, 0)
            out['expiring_soon'] = {
                'pos': row[0] or 0,
                'units': float(row[1] or 0),
                'liters': float(row[2] or 0),
                'within_days': MIN_DAYS_TO_EXPIRY,
            }

        # ── Headline figures, derived from the blocks above ──────────────────
        appt = out['appointments_by_day']
        ship = out['shipments_by_day']
        pend = out['pendency_by_fc']
        appt_total = sum(d['total'] for d in appt)
        ship_count = sum(d['shipments'] for d in ship)
        # Weighted by shipments per day, so a day with one truck does not swing
        # the average as hard as a day with twelve.
        weighted_fill = sum(d['avg_fill'] * d['shipments'] for d in ship)
        out['kpis'] = {
            'appointments': appt_total,
            'confirmed': sum(d['confirmed'] for d in appt),
            'cancelled': sum(d['cancelled'] for d in appt),
            'cancel_rate': round(sum(d['cancelled'] for d in appt) / appt_total * 100, 1)
            if appt_total else 0.0,
            'shipments': ship_count,
            'planned_liters': round(sum(d['liters'] for d in ship), 1),
            'planned_units': round(sum(r['units'] for r in out['volume_by_fc']), 0),
            'avg_fill': round(weighted_fill / ship_count, 1) if ship_count else 0.0,
            'pending_pos': sum(r['pos'] for r in pend),
            'pending_units': round(sum(r['units'] for r in pend), 0),
            'pending_liters': round(sum(r['liters'] for r in pend), 1),
        }

        # Filter options come from the FC master, so the bar can never offer an
        # FC the data does not know about.
        out['options'] = {
            'fcs': sorted(fc_channel.keys()),
            'channels': sorted({ch for ch in fc_channel.values() if ch}),
            'fc_channel': fc_channel,
        }
        return Response(out)
