"""New Shipment 2.0 — a self-contained planning surface.

INDEPENDENT BY CONSTRUCTION. Everything here lives under its own URL namespace
(``/api/shipment/v2/``) in its own module, and not one line of the original
wizard's views, URLs or SQL is edited to make it work. Turning 2.0 off is
deleting one ``include()``.

NOT independent in its ARITHMETIC, deliberately. The free-stock figure, the
days-of-cover figure, the near-expiry cutoff, the 20-unit minimum line and the
packer itself are all imported from ``shipment.views``. Two planning screens
quoting different free-stock numbers for the same ASIN is the kind of
contradiction that costs an afternoon to chase, so 2.0 re-uses the tested
helpers rather than growing a second opinion. What is genuinely new here is the
SHAPE of the flow — channel first, then one appointment, then the channel's
whole open book PO-wise — and only that part is written from scratch.

The screens map onto the endpoints:

    channels/       Core / Now / Fresh, each with what is open behind it
    appointments/   every appointment on the chosen channel, as cards
    pos/            the channel's open book, grouped PO-wise, pendency columns
    fill/options/   which families / item heads / truck sizes are offerable
    fill/           build a truck from that book under the chosen strategies

Nothing here writes. ``fill/`` returns a plan; it does not create a shipment.

── A NOTE ON TABLE ALIASES ──────────────────────────────────────────────────
The SKU PO Pendency expressions this module imports are written against the
table's FULL NAME (``reporting."Amazon PO".po_number``), not an alias, because
their correlated sub-selects have to name the outer row somehow. Aliasing the
table would REPLACE that name and every one of those references would fail to
resolve. So any query that uses a pendency expression selects from
``reporting."Amazon PO"`` unaliased, and the channel helper below has an
unaliased twin for exactly that case.
"""

from __future__ import annotations

import logging

from django.db import connection, transaction
from django.utils import timezone
from rest_framework.permissions import IsAuthenticated
from rest_framework.response import Response

from .views import (
    MIN_AUTO_LINE_UNITS,
    PRODUCT_FAMILIES,
    _apply_stock_caps,
    _auto_plan_truck,
    _BILLING_JOIN,
    _compute_priority,
    _doh_fill_sort_key,
    _even_share_key,
    _family_sql,
    _fill_sort_key,
    _live_doh_by_asin,
    _normalise_families,
    _pack_into_capacity,
    _planner_stock_detail,
    _reserved_stock_by_asin,
    _row_to_dict,
    _SafeAPIView,
    _safe_int,
    _serialize_row,
    _stock_meta_payload,
    _SUGGESTION_ADD_STALE_KEYS,
    _tag_expiry_warnings,
)

logger = logging.getLogger(__name__)


# ── Channels ─────────────────────────────────────────────────────────────────
# The three Amazon lanes. Written as a fixed tuple rather than "whatever is in
# the data" so the first screen always shows three buttons in the same order: a
# channel with nothing open today is a real answer ("nothing to plan on Now"),
# and it has to be visible to be read as one.
V2_CHANNELS = ('CORE', 'NOW', 'FRESH')


def _channel_case(col: str) -> str:
    """Normalise a channel column to CORE / NOW / FRESH / UNMAPPED.

    Both sides of the flow spell the channel slightly differently in places
    ("Core", "AMAZON NOW", "Fresh "), so every comparison goes through this
    ladder rather than a bare equality — the same rule the platform dashboards
    apply.

    STRPOS rather than LIKE, on purpose: a LIKE pattern needs its ``%`` doubled
    to survive psycopg2's parameter interpolation, and then only when the query
    happens to pass parameters. That is a footgun that fires as a silently wrong
    channel rather than an error, and this expression is pasted into six
    queries. STRPOS has no metacharacters, so it reads the same everywhere.
    """
    up = f"UPPER(TRIM(COALESCE({col}::text, '')))"
    return (
        "CASE"
        f" WHEN STRPOS({up}, 'FRESH') > 0 THEN 'FRESH'"
        f" WHEN STRPOS({up}, 'NOW')   > 0 THEN 'NOW'"
        f" WHEN STRPOS({up}, 'CORE')  > 0 THEN 'CORE'"
        f" ELSE COALESCE(NULLIF({up}, ''), 'UNMAPPED')"
        " END"
    )


# Appointment side — the FC master's channel. The _AGG twin is for queries that
# GROUP BY appointment_id, where a bare `fcm.channel` is neither grouped nor
# aggregated and Postgres refuses it.
_APPT_CHANNEL = _channel_case('fcm.channel')
_APPT_CHANNEL_AGG = _channel_case('MAX(fcm.channel)')

# PO side — the sheet's own core_fresh_now. Two spellings for the two alias
# regimes described in the module docstring.
_PO_CHANNEL = _channel_case('p.core_fresh_now')
_PO_CHANNEL_RAW = _channel_case('core_fresh_now')

def _item_head_case(col: str = 'item_head') -> str:
    """The item-head split in SQL.

    Character-for-character the same three-way test as
    ``shipment.views._item_head_bucket``: PREMIUM wins, then COMMODITY, then
    everything else is OTHER. The packer buckets lines with that function, so a
    SQL summary bucketing them differently would advertise capacity for a bucket
    the packer then refuses to fill.
    """
    up = f"UPPER(TRIM(COALESCE({col}, '')))"
    return (
        "CASE"
        f" WHEN STRPOS({up}, 'PREMIUM')   > 0 THEN 'PREMIUM'"
        f" WHEN STRPOS({up}, 'COMMODITY') > 0 THEN 'COMMODITY'"
        " ELSE 'OTHER'"
        " END"
    )


def _norm_channel(raw) -> str:
    """A request's ?channel= as one of V2_CHANNELS, or '' for "no filter"."""
    text = str(raw or '').strip().upper()
    if not text:
        return ''
    for name in V2_CHANNELS:
        if name in text:
            return name
    return text


def _pendency_sql():
    """The open book, in the SKU PO Pendency page's own words.

    Imported lazily for the reason that page's own helper states: the import
    graph runs one way (uploads imports shipment), and reversing it at module
    level would close a cycle. Everything 2.0 shows as a "pendency column" comes
    through here, so the two pages can never disagree about what is open.
    """
    from uploads.amazon_uploads import (
        _PENDENCY_INVOICE_COUNT,
        _PENDENCY_INVOICE_DETAIL,
        _PENDENCY_INVOICE_NOS,
        _PENDENCY_INVOICED_LTRS,
        _PENDENCY_INVOICED_QTY,
        _PENDENCY_INVOICED_STATUS,
        _PENDENCY_SHORT_LTRS,
        _PENDENCY_SHORT_QTY,
        _SKU_PENDENCY_FULLY_INVOICED,
        _SKU_PENDENCY_HAS_INVOICE,
        _SKU_PENDENCY_HAS_STATED_LITRE,
        _SKU_PENDENCY_IS_DISPATCHED,
        _SKU_PENDENCY_PENDING,
        _stated_litres,
        SKU_PENDENCY_COLUMNS,
    )
    return {
        'columns': SKU_PENDENCY_COLUMNS,
        'pending': _SKU_PENDENCY_PENDING,
        'fully_invoiced': _SKU_PENDENCY_FULLY_INVOICED,
        'exprs': {
            'has_stated_litre': _SKU_PENDENCY_HAS_STATED_LITRE,
            'has_invoice': _SKU_PENDENCY_HAS_INVOICE,
            'is_dispatched': _SKU_PENDENCY_IS_DISPATCHED,
            'invoiced_status': _PENDENCY_INVOICED_STATUS,
            'invoiced_qty': _PENDENCY_INVOICED_QTY,
            'invoiced_short_qty': _PENDENCY_SHORT_QTY,
            'invoiced_ltrs': _PENDENCY_INVOICED_LTRS,
            'invoiced_short_ltrs': _PENDENCY_SHORT_LTRS,
            'invoice_nos': _PENDENCY_INVOICE_NOS,
            'invoice_count': _PENDENCY_INVOICE_COUNT,
            'invoice_detail': _PENDENCY_INVOICE_DETAIL,
            'total_order_liters': _stated_litres('total_order_liters'),
            'total_accepted_liters': _stated_litres('total_accepted_liters'),
            'total_delivered_liters': _stated_litres('total_delivered_liters'),
            'remaining_ltrs': _stated_litres('remaining_ltrs'),
        },
    }


def _enrich_stock(payload):
    from uploads.amazon_uploads import _enrich_pendency_stock
    return _enrich_pendency_stock(payload)


def _num(value, default=0.0):
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _no_jit(cur):
    """Turn Postgres JIT off for the current transaction.

    These queries build a wide SELECT list of correlated sub-selects (the
    invoiced split, the dispatch flag, the stated-litre gate), which inflates
    the planner's cost estimate past jit_above_cost and makes Postgres
    LLVM-compile the whole thing — measured on the pendency page at 9.5s of
    compilation wrapped around 180ms of work. SET LOCAL is scoped to the
    transaction, which is why every caller wraps itself in one: outside a
    transaction block Postgres discards it with a warning and the cost comes
    straight back.
    """
    cur.execute('SET LOCAL jit = off')


# ─────────────────────────────────────────────────────────────────────────────
# 1. Channels
# ─────────────────────────────────────────────────────────────────────────────

class V2ChannelsView(_SafeAPIView):
    """The three lane buttons, each carrying what is actually behind it.

    Two independent counts per channel, from the two tables the flow spans:
    appointments (booked slots) and open PO lines (the book to load from). They
    are deliberately NOT joined — a channel can have open POs and no
    appointment, or an appointment and nothing left to load, and both facts are
    worth seeing before the click rather than after it.
    """

    permission_classes = [IsAuthenticated]

    def get(self, request):
        today = timezone.localdate()
        sql = _pendency_sql()

        with transaction.atomic():
            with connection.cursor() as cur:
                _no_jit(cur)
                cur.execute(f"""
                    WITH appt AS (
                        SELECT a.appointment_id,
                               MAX(a.appointment_time) AS appointment_time,
                               MAX(a.destination_fc)   AS destination_fc
                        FROM reporting."appointment" a
                        WHERE a.appointment_time IS NOT NULL
                        GROUP BY a.appointment_id
                    )
                    SELECT {_APPT_CHANNEL}                        AS channel,
                           COUNT(DISTINCT appt.appointment_id)     AS appointment_count,
                           COUNT(DISTINCT appt.appointment_id) FILTER (
                               WHERE DATE(appt.appointment_time) >= %s
                           )                                       AS upcoming_count,
                           COUNT(DISTINCT UPPER(TRIM(COALESCE(appt.destination_fc, ''))))
                                                                   AS fc_count,
                           MIN(appt.appointment_time) FILTER (
                               WHERE DATE(appt.appointment_time) >= %s
                           )                                       AS next_appointment_time
                    FROM appt
                    -- LEFT JOIN, so an FC missing from the master still lands
                    -- somewhere (UNMAPPED) instead of vanishing from every count.
                    LEFT JOIN public.fc_city_state_channel_master fcm
                           ON UPPER(TRIM(fcm.fc::text))
                            = UPPER(TRIM(COALESCE(appt.destination_fc, '')::text))
                    GROUP BY 1
                """, [today, today])
                appt_rows = {r['channel']: r for r in _row_to_dict(cur, cur.fetchall())}

                # The open book per channel, counted with the pendency page's own
                # gate: PENDING, not finished, not fully invoiced. Same predicate
                # the `pos/` endpoint lists with, so the number on the button is
                # the number of lines the next screen shows.
                cur.execute(f"""
                    SELECT {_PO_CHANNEL_RAW}                    AS channel,
                           COUNT(*)                             AS open_line_count,
                           COUNT(DISTINCT UPPER(TRIM(po_number))) AS open_po_count,
                           COUNT(DISTINCT UPPER(TRIM(sku_code)))  AS open_sku_count,
                           COALESCE(SUM(GREATEST(COALESCE(remaining_qty, 0), 0)), 0)  AS open_units,
                           COALESCE(SUM(GREATEST(COALESCE(remaining_ltrs, 0), 0)), 0) AS open_liters
                    FROM reporting."Amazon PO"
                    WHERE {sql['pending']}
                      AND NOT {sql['fully_invoiced']}
                    GROUP BY 1
                """, [])
                po_rows = {r['channel']: r for r in _row_to_dict(cur, cur.fetchall())}

        out = []
        for name in V2_CHANNELS:
            a = appt_rows.get(name) or {}
            p = po_rows.get(name) or {}
            out.append(_serialize_row({
                'channel': name,
                'appointment_count': int(a.get('appointment_count') or 0),
                'upcoming_count': int(a.get('upcoming_count') or 0),
                'fc_count': int(a.get('fc_count') or 0),
                'next_appointment_time': a.get('next_appointment_time'),
                'open_line_count': int(p.get('open_line_count') or 0),
                'open_po_count': int(p.get('open_po_count') or 0),
                'open_sku_count': int(p.get('open_sku_count') or 0),
                'open_units': _num(p.get('open_units')),
                'open_liters': _num(p.get('open_liters')),
            }))
        return Response({'channels': out, 'as_of': today.isoformat()})


# ─────────────────────────────────────────────────────────────────────────────
# 2. Appointments on a channel
# ─────────────────────────────────────────────────────────────────────────────

class V2AppointmentsView(_SafeAPIView):
    """Every appointment on one channel, as cards.

    ``scope`` is the only parameter that changes the ANSWER rather than
    narrowing it: 'upcoming' (default) is today onward, 'past' is behind us,
    'all' is both. Cancelled slots are never hidden — an appointment's status is
    one of the things the card exists to state.

    Vendor Central's own unit / carton commitment rides along from
    ``public.appointment_commit``. Where VC has no carton figure the cartons are
    ESTIMATED from the appointment's PO lines (accepted_qty / case_pack) and
    flagged ``carton_is_calc`` — the same rule and the same flag the existing
    appointment list uses, so the two screens never quote different cartons for
    one appointment. Units are never estimated: there is no honest way to derive
    a commitment Amazon did not give.
    """

    permission_classes = [IsAuthenticated]

    def get(self, request):
        channel = _norm_channel(request.query_params.get('channel'))
        scope = str(request.query_params.get('scope') or 'upcoming').strip().lower()
        if scope not in ('upcoming', 'past', 'all'):
            scope = 'upcoming'
        search = str(request.query_params.get('search') or '').strip()
        limit = _safe_int(request.query_params.get('limit'), 300, lo=1, hi=1000)
        today = timezone.localdate()

        where = ['a.appointment_time IS NOT NULL']
        params: list = []
        if scope == 'upcoming':
            where.append('DATE(a.appointment_time) >= %s')
            params.append(today)
        elif scope == 'past':
            where.append('DATE(a.appointment_time) < %s')
            params.append(today)

        # Both filters run in HAVING, not WHERE: the channel is only known after
        # the FC join is aggregated (see _APPT_CHANNEL_AGG), and the PO search has
        # to see the whole stitched PO list rather than one of the per-PO rows the
        # ingest actually stores.
        having = []
        having_params: list = []
        if channel:
            having.append(f'{_APPT_CHANNEL_AGG} = %s')
            having_params.append(channel)
        if search:
            having.append(
                "(a.appointment_id ILIKE %s"
                " OR MAX(a.destination_fc) ILIKE %s"
                " OR STRING_AGG(DISTINCT COALESCE(a.pos, ''), ',') ILIKE %s)"
            )
            having_params.extend([f'%{search[:120]}%'] * 3)

        where_sql = ' AND '.join(where)
        having_sql = (' HAVING ' + ' AND '.join(having)) if having else ''
        direction = 'ASC' if scope == 'upcoming' else 'DESC'

        with connection.cursor() as cur:
            cur.execute(f"""
                SELECT a.appointment_id,
                       MAX(a.status)           AS status,
                       MAX(a.appointment_time) AS appointment_time,
                       MAX(a.creation_date)    AS creation_date,
                       MAX(a.destination_fc)   AS destination_fc,
                       MAX(a.pro)              AS pro,
                       {_APPT_CHANNEL_AGG}     AS channel,
                       -- The ingest stores one row per (appointment, PO); stitch
                       -- them back into one card.
                       STRING_AGG(
                           DISTINCT NULLIF(TRIM(COALESCE(a.pos, '')), ''),
                           ', ' ORDER BY NULLIF(TRIM(COALESCE(a.pos, '')), '')
                       )                       AS pos,
                       COUNT(DISTINCT NULLIF(TRIM(COALESCE(a.pos, '')), '')) AS po_count,
                       MAX(acm.carton_count)   AS amazon_carton_count,
                       MAX(acm.unit_count)     AS amazon_unit_count,
                       -- Carton fallback. `pos` is a comma/semicolon CSV, so it is
                       -- split before matching: a multi-PO appointment otherwise
                       -- matches nothing and reads as zero cartons rather than as
                       -- "no figure".
                       (
                           SELECT ROUND(SUM(p2.accepted_qty::numeric
                                            / GREATEST(p2.case_pack, 1)))
                           FROM reporting."Amazon PO" p2
                           WHERE UPPER(TRIM(p2.po_number)) IN (
                               SELECT UPPER(TRIM(pv))
                               FROM reporting."appointment" a2,
                                    LATERAL unnest(regexp_split_to_array(
                                        COALESCE(a2.pos, ''), '\\s*[,;]\\s*')) AS pv
                               WHERE a2.appointment_id = a.appointment_id
                                 AND NULLIF(TRIM(pv), '') IS NOT NULL
                           )
                       )                       AS calc_carton_count
                FROM reporting."appointment" a
                LEFT JOIN public.appointment_commit acm
                       ON acm.appointment_id = a.appointment_id
                LEFT JOIN public.fc_city_state_channel_master fcm
                       ON UPPER(TRIM(fcm.fc::text))
                        = UPPER(TRIM(COALESCE(a.destination_fc, '')::text))
                WHERE {where_sql}
                GROUP BY a.appointment_id
                {having_sql}
                ORDER BY MAX(a.appointment_time) {direction} NULLS LAST
                LIMIT %s
            """, params + having_params + [limit])
            rows = _row_to_dict(cur, cur.fetchall())

        # How much of each appointment is still OPEN. The card's headline is
        # Amazon's commitment, but a slot whose POs are all invoiced is not
        # something to plan against, and the card should say so before it is
        # clicked rather than after the next screen comes back empty.
        po_uppers: set[str] = set()
        per_appt_pos: dict[str, set[str]] = {}
        for r in rows:
            codes = {
                c.strip().upper()
                for c in str(r.get('pos') or '').replace(';', ',').split(',')
                if c.strip()
            }
            per_appt_pos[r['appointment_id']] = codes
            po_uppers |= codes

        open_by_po = {}
        if po_uppers:
            sql = _pendency_sql()
            with transaction.atomic():
                with connection.cursor() as cur:
                    _no_jit(cur)
                    cur.execute(f"""
                        SELECT UPPER(TRIM(po_number)) AS po,
                               COUNT(*) AS line_count,
                               COALESCE(SUM(GREATEST(COALESCE(remaining_qty, 0), 0)), 0)  AS units,
                               COALESCE(SUM(GREATEST(COALESCE(remaining_ltrs, 0), 0)), 0) AS liters
                        FROM reporting."Amazon PO"
                        WHERE UPPER(TRIM(po_number)) = ANY(%s::text[])
                          AND {sql['pending']}
                          AND NOT {sql['fully_invoiced']}
                        GROUP BY 1
                    """, [sorted(po_uppers)])
                    open_by_po = {r['po']: r for r in _row_to_dict(cur, cur.fetchall())}

        results = []
        for r in rows:
            cc = r.get('amazon_carton_count')
            calc_raw = r.pop('calc_carton_count', None)
            calc = None
            if cc is None and calc_raw is not None:
                try:
                    calc = int(round(float(calc_raw)))
                    if calc <= 0:
                        calc = None
                except (TypeError, ValueError):
                    calc = None
            r['amazon_carton_count_calc'] = calc
            r['carton_is_calc'] = calc is not None

            codes = per_appt_pos.get(r['appointment_id'], set())
            open_rows = [open_by_po[c] for c in codes if c in open_by_po]
            r['po_codes'] = sorted(codes)
            r['open_po_count'] = len(open_rows)
            r['open_line_count'] = sum(int(o['line_count'] or 0) for o in open_rows)
            r['open_units'] = sum(_num(o['units']) for o in open_rows)
            r['open_liters'] = sum(_num(o['liters']) for o in open_rows)
            results.append(_serialize_row(r))

        return Response({
            'results': results,
            'count': len(results),
            'channel': channel,
            'scope': scope,
            'as_of': today.isoformat(),
        })


# ─────────────────────────────────────────────────────────────────────────────
# 3. The channel's open book, PO-wise
# ─────────────────────────────────────────────────────────────────────────────

# Line-level fields that summarise up to the PO header. One list, so the header
# and the rows under it can never sum different columns.
_PO_ROLLUP = (
    ('units', 'remaining_qty'),
    ('liters', 'remaining_ltrs'),
    ('ordered_units', 'requested_qty'),
    ('accepted_units', 'accepted_qty'),
    ('received_units', 'received_qty'),
    ('invoiced_units', 'invoiced_qty'),
)


def _appointment_pos(appointment_id):
    """(set of PO codes on this appointment, its FC). Empty when none given."""
    if not appointment_id:
        return set(), ''
    with connection.cursor() as cur:
        cur.execute("""
            SELECT STRING_AGG(DISTINCT NULLIF(TRIM(COALESCE(pos, '')), ''), ',') AS pos,
                   MAX(destination_fc) AS fc
            FROM reporting."appointment"
            WHERE appointment_id = %s
        """, [appointment_id])
        row = cur.fetchone()
    if not row:
        return set(), ''
    codes = {
        c.strip().upper()
        for c in str(row[0] or '').replace(';', ',').split(',')
        if c.strip()
    }
    return codes, str(row[1] or '')


class V2PoBookView(_SafeAPIView):
    """Every OPEN PO line on the channel, grouped PO-wise.

    Same source, same gate and the same columns as SKU PO Pendency's "Opened PO"
    bucket — this is that page's data, re-shaped into PO → SKU rather than one
    flat table, and enriched with the planner's live free stock and DOH by the
    pendency page's own helper.

    The chosen appointment does NOT filter this list; it TAGS it. Its own POs
    come back with ``on_appointment`` set and sort to the front, and everything
    else on the channel is still listed and still loadable. That is the whole
    difference from the original wizard, where the appointment defined the
    candidate pool.
    """

    permission_classes = [IsAuthenticated]

    def get(self, request):
        channel = _norm_channel(request.query_params.get('channel'))
        appointment_id = str(request.query_params.get('appointment_id') or '').strip()
        search = str(request.query_params.get('search') or '').strip()
        fc = str(request.query_params.get('fc') or '').strip()
        # A hard ceiling rather than paging: the whole open book is ~1k lines,
        # and PO-wise grouping only makes sense over the complete set — a page
        # boundary would cut a PO in half and its header would then lie.
        limit = _safe_int(request.query_params.get('limit'), 5000, lo=1, hi=20000)

        sql = _pendency_sql()
        where = [sql['pending'], f"NOT {sql['fully_invoiced']}"]
        params: list = []
        if channel:
            where.append(f"{_PO_CHANNEL_RAW} = %s")
            params.append(channel)
        if fc:
            where.append("UPPER(TRIM(COALESCE(fulfillment_center, ''))) = %s")
            params.append(fc.upper())
        if search:
            where.append(
                "(po_number ILIKE %s OR sku_code ILIKE %s OR asin ILIKE %s"
                " OR item ILIKE %s OR category ILIKE %s OR item_head ILIKE %s"
                " OR fulfillment_center ILIKE %s)"
            )
            params.extend([f'%{search[:200]}%'] * 7)

        cols = ', '.join(
            f'{sql["exprs"][c]} AS "{c}"' if c in sql['exprs'] else f'"{c}"'
            for c in sql['columns']
        )

        with transaction.atomic():
            with connection.cursor() as cur:
                _no_jit(cur)
                # Unaliased FROM — see the module docstring. `asin`, `case_pack`
                # and the rest are not pendency columns but the fill endpoint and
                # the stock enrichment key on them, so they are selected here.
                cur.execute(f"""
                    SELECT {cols},
                           asin,
                           case_pack,
                           category,
                           sub_category,
                           days_to_expiry,
                           {_PO_CHANNEL_RAW} AS channel
                    FROM reporting."Amazon PO"
                    WHERE {' AND '.join(where)}
                    ORDER BY po_number ASC, expiry_date ASC NULLS LAST, sku_code ASC
                    LIMIT %s
                """, params + [limit])
                rows = _row_to_dict(cur, cur.fetchall())

        payload = _enrich_stock({'results': [_serialize_row(r) for r in rows]})
        lines = payload.get('results', [])

        appt_pos, appt_fc = _appointment_pos(appointment_id)

        groups: dict[str, dict] = {}
        for line in lines:
            code = str(line.get('po_number') or '').strip()
            key = code.upper()
            g = groups.get(key)
            if g is None:
                g = {
                    'po_number': code,
                    'order_date': line.get('order_date'),
                    'expiry_date': line.get('expiry_date'),
                    'fulfillment_center': line.get('fulfillment_center'),
                    'channel': line.get('channel'),
                    'item_heads': set(),
                    'categories': set(),
                    'sku_count': 0,
                    'on_appointment': key in appt_pos,
                    'lines': [],
                }
                for name, _src in _PO_ROLLUP:
                    g[name] = 0.0
                groups[key] = g
            g['lines'].append(line)
            g['sku_count'] += 1
            if line.get('item_head'):
                g['item_heads'].add(str(line['item_head']).strip())
            if line.get('category'):
                g['categories'].add(str(line['category']).strip())
            for name, src in _PO_ROLLUP:
                g[name] += _num(line.get(src))
            # The PO's own deadline is the SOONEST of its lines'. The PO cancels
            # when its first line does, so a header carrying the latest date
            # would read as safe while half the PO is already past saving.
            ed = line.get('expiry_date')
            if ed and (not g['expiry_date'] or str(ed) < str(g['expiry_date'])):
                g['expiry_date'] = ed

        out = []
        for g in groups.values():
            g['item_heads'] = sorted(g.pop('item_heads'))
            g['categories'] = sorted(g.pop('categories'))
            g['item_head'] = g['item_heads'][0] if len(g['item_heads']) == 1 else ''
            # HOW MUCH OF THIS PO THE WAREHOUSE CAN COVER — not how much stock
            # exists. Two corrections, both needed for the header to mean
            # anything beside the PO's own outstanding units:
            #
            #   per ASIN, not per line — an ASIN on two lines of one PO has ONE
            #   pool behind it, and summing the column would promise it twice;
            #   capped at what the PO needs — free stock is warehouse-wide, so an
            #   uncapped sum reads "91,913 in stock" against a PO wanting 2,803
            #   and says nothing about whether the PO can ship.
            need, free = {}, {}
            for line in g['lines']:
                a = str(line.get('asin') or line.get('sku_code') or '').strip().upper()
                if not a:
                    continue
                need[a] = need.get(a, 0.0) + max(0.0, _num(line.get('remaining_qty')))
                if line.get('gp_stock') is not None:
                    free[a] = _num(line.get('gp_stock'))
            g['stock_backed_units'] = round(
                sum(min(need[a], free.get(a, 0.0)) for a in need), 2)
            # Whether every ASIN on the PO is even MAPPED to warehouse stock.
            # A PO reading 0% could be out of stock or simply unmapped, and only
            # one of those is a stock problem.
            g['stock_unmapped_skus'] = sum(1 for a in need if a not in free)
            total_need = sum(need.values())
            g['stock_cover_pct'] = (
                round(g['stock_backed_units'] / total_need * 100, 1)
                if total_need > 0 else None
            )
            for name, _src in _PO_ROLLUP:
                g[name] = round(g[name], 4)
            out.append(g)

        # Appointment POs first, then soonest deadline, then PO code — so the slot
        # you picked opens at the top without anything else being hidden.
        out.sort(key=lambda g: (
            0 if g['on_appointment'] else 1,
            str(g.get('expiry_date') or '9999-12-31'),
            str(g.get('po_number') or ''),
        ))

        totals = {name: round(sum(g[name] for g in out), 4) for name, _s in _PO_ROLLUP}
        totals['po_count'] = len(out)
        totals['line_count'] = len(lines)
        # Summed across POs this is an OVER-count where two POs share an ASIN —
        # each was capped against its own need, not against one shared pool. It
        # is an upper bound on what could ship, which is what a book-level
        # "coverable" figure can honestly be; the truck's real answer comes from
        # the fill endpoint, where the pool is drained line by line.
        totals['stock_backed_units'] = round(sum(g['stock_backed_units'] for g in out), 2)

        return Response({
            'pos': out,
            'totals': totals,
            'channel': channel,
            'appointment_id': appointment_id,
            'appointment_fc': appt_fc,
            'appointment_po_count': sum(1 for g in out if g['on_appointment']),
            # The ceiling is a real cap, so say when it bit rather than letting a
            # truncated book read as the whole book.
            'truncated': len(lines) >= limit,
        })


# ─────────────────────────────────────────────────────────────────────────────
# 4. Express fill
# ─────────────────────────────────────────────────────────────────────────────

# What the Express panel can ask for. Several may be on at once and they compose
# on different axes, which is the point of a multi-select:
#
#   doh             ORDER. Lowest days-of-cover first instead of biggest line.
#   with_stock      QUANTITY. Cap every line to live free stock (the default).
#   without_stock   QUANTITY. Plan the ordered quantity even with no stock behind
#                   it. With `with_stock` too it becomes a SECOND PASS: the truck
#                   fills from stock first and only the leftover capacity is
#                   topped up unbacked, so an unbacked line can never displace one
#                   that could ship today.
#   focus           POOL. Restrict to chosen product families / packs, and split
#                   the truck evenly between them.
#   priority        SLICE. Carve capacity by item head (Premium / Commodity /
#                   Other) and pack each slice from its own bucket.
V2_STRATEGIES = ('doh', 'with_stock', 'without_stock', 'focus', 'priority')

# Truck sizes the picker offers, in litres (1 tonne = 1000 L — the convention the
# TONNES column and every planner label already use). The endpoint accepts any
# capacity in range, so these are a convenience, not a constraint.
V2_TRUCK_PRESETS = (10000.0, 15000.0, 18000.0, 20000.0)
V2_MIN_CAPACITY = 500.0
V2_MAX_CAPACITY = 40000.0


def _normalise_strategies(raw) -> list[str]:
    """The requested strategies, de-duplicated, in a fixed order.

    Unknown names are dropped rather than rejected, for the same reason the
    product-family parser drops them: a strategy this build does not know must
    leave the fill behaving as though it had not been asked for, never plan
    against a rule nobody defined.
    """
    if isinstance(raw, str):
        raw = raw.split(',')
    wanted = {str(x).strip().lower() for x in (raw or []) if str(x).strip()}
    return [s for s in V2_STRATEGIES if s in wanted]


def _normalise_priority(raw):
    """{'PREMIUM': pct, 'COMMODITY': pct, 'OTHER': pct} or None.

    Rescaled only when the three do not sum to 100 — the packer carves capacity
    by these directly, so an unnormalised 40/40/40 would hand out 120% of the
    truck and the first two buckets would eat the third's slice.
    """
    if not isinstance(raw, dict):
        return None
    out = {}
    for key, src in (('PREMIUM', 'premium'), ('COMMODITY', 'commodity'), ('OTHER', 'other')):
        out[key] = max(0.0, min(100.0, _num(raw.get(src), 0.0)))
    total = sum(out.values())
    if total <= 0:
        return None
    if abs(total - 100.0) > 0.01:
        out = {k: round(v * 100.0 / total, 4) for k, v in out.items()}
    return out


# The candidate pool's CTEs and its base predicate, shared by the fill itself and
# by the Express panel's family counts. They MUST be the same rules: a panel that
# offers "MUSTARD · 24 POs · 177,881 L" from a looser query, and then plans 259 L
# because most of it is locked to a live shipment or already dispatched, has not
# given the planner a choice — it has given them a wrong number to choose from.
_POOL_CTES = f"""
    WITH locked_pairs AS (
        SELECT DISTINCT si.asin, UPPER(TRIM(si.po_number)) AS po_number
        FROM sp_items si
        JOIN sp_shipments s ON s.id = si.shipment_id
        WHERE si.not_loaded = FALSE
          AND s.status != 'rejected'
    ),
    billed AS (
        -- SAP-billed units per PO+item, split greedily across sibling ASINs that
        -- share a sap_sku_code so the pool is consumed once. Same rule as every
        -- other planner query.
        SELECT
            UPPER(TRIM(ap.po_number)) AS po_number,
            ap.asin,
            LEAST(
                ap.accepted_qty,
                GREATEST(
                    sb.dispatched_qty - COALESCE(SUM(ap.accepted_qty) OVER (
                        PARTITION BY UPPER(TRIM(ap.po_number)),
                                     UPPER(TRIM(ap.sap_sku_code))
                        ORDER BY ap.asin
                        ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING), 0),
                    0
                )
            ) AS billed_qty,
            (sb.billed_qty > sb.dispatched_qty) AS has_invoiced
        FROM reporting."Amazon PO" ap
        {_BILLING_JOIN}
            ON sb.po_number = UPPER(TRIM(ap.po_number))
           AND sb.sap_item_code = UPPER(TRIM(ap.sap_sku_code))
        WHERE ap.accepted_qty > 0
    )
"""

_POOL_JOINS = """
    LEFT JOIN locked_pairs lp
           ON lp.asin = p.asin
          AND lp.po_number = UPPER(TRIM(p.po_number))
    LEFT JOIN billed b
           ON b.po_number = UPPER(TRIM(p.po_number))
          AND b.asin = p.asin
"""

_POOL_WHERE = (
    "p.status = 'Confirmed'",
    "p.availability_status = 'AC - Accepted: In stock'",
    "p.accepted_qty > 0",
    "p.po_status = 'PENDING'",
    # The packer hard-filters on volume, so a line with no per-litre value would
    # be fetched only to be refused with a confusing reason.
    "p.per_liter IS NOT NULL",
    "p.per_liter > 0",
    "lp.asin IS NULL",
    "(p.accepted_qty - COALESCE(b.billed_qty, 0)) > 0",
)

# Units still shippable on a pool row: ordered, less what SAP already dispatched.
_POOL_UNITS = 'GREATEST(p.accepted_qty - COALESCE(b.billed_qty, 0), 0)'


def _fill_candidates(channel, fc_preference, selected_pos, families, asins):
    """The channel's shippable lines, ready to pack.

    The same eligibility rules the original planner's filler pool applies —
    Confirmed, in stock at Amazon, PENDING, a per-litre value on record, net of
    what SAP already dispatched, and not already locked to another live shipment
    — but scoped to a CHANNEL rather than a single FC, which is the one thing
    2.0 changes about where a truck may draw from.

    ``fc_preference`` (the chosen appointment's FC) does not filter anything. It
    is stamped onto each line as ``is_switch``, which is the FIRST term of both
    fill orders: everything already at the truck's own FC loads before anything
    that would have to be switched in from a sister FC on the same channel,
    however large or however urgent. With no appointment chosen there is no home
    FC, the flag is off across the board, and the orders fall back to pure size
    or pure urgency.
    """
    family_sql, family_params = _family_sql(families)
    asin_list = [str(a).strip().upper() for a in (asins or []) if str(a).strip()]
    po_list = [str(p).strip().upper() for p in (selected_pos or []) if str(p).strip()]

    where = list(_POOL_WHERE)
    params: list = []
    if channel:
        where.append(f"{_PO_CHANNEL} = %s")
        params.append(channel)
    if po_list:
        where.append("UPPER(TRIM(p.po_number)) = ANY(%s::text[])")
        params.append(po_list)
    if family_sql:
        where.append(family_sql)
        params.extend(family_params)
    if asin_list:
        where.append("UPPER(TRIM(p.asin)) = ANY(%s::text[])")
        params.append(asin_list)

    with transaction.atomic():
        with connection.cursor() as cur:
            _no_jit(cur)
            cur.execute(f"""
                {_POOL_CTES}
                SELECT
                    p.po_number,
                    p.asin,
                    p.merchant_sku  AS internal_sku,
                    p.sku_code,
                    p.sap_sku_code,
                    p.sku_name      AS product_name,
                    p.item,
                    {_POOL_UNITS} AS accepted_qty,
                    -- The quantity BEFORE billing was taken off. Without it the
                    -- invoice popup subtracts the billed amount a second time and
                    -- reports 0 left to ship on a line the planner just loaded.
                    p.accepted_qty  AS original_accepted_qty,
                    COALESCE(b.billed_qty, 0)       AS billed_qty,
                    COALESCE(b.has_invoiced, FALSE) AS has_invoiced,
                    p.requested_qty,
                    p.received_qty,
                    p.cancelled_qty,
                    p.remaining_qty,
                    p.fulfillment_center,
                    p.fulfillment_center AS destination_fc,
                    0::numeric      AS committed_qty,
                    p.case_pack,
                    p.per_liter,
                    p.cost_price,
                    round({_POOL_UNITS} * COALESCE(p.per_liter, 0), 4)
                                    AS total_accepted_liters,
                    p.order_date,
                    p.days_to_expiry,
                    p.expiry_date,
                    p.category,
                    p.sub_category,
                    p.brand,
                    p.item_head,
                    p.availability_status,
                    p.po_status,
                    p.status,
                    {_PO_CHANNEL} AS channel
                FROM reporting."Amazon PO" p
                {_POOL_JOINS}
                WHERE {' AND '.join(where)}
                -- Just so the pool never arrives in an arbitrary order; the sort
                -- keys applied in Python are the authority.
                ORDER BY {_POOL_UNITS} DESC, p.po_number, p.asin
            """, params)
            raw = _row_to_dict(cur, cur.fetchall())

    doh_by_asin, doh_meta = _live_doh_by_asin()
    fc_pref = str(fc_preference or '').strip().upper()

    pool = []
    for r in raw:
        row = _serialize_row(r)
        asin_up = str(row.get('asin') or '').upper().strip()
        live = doh_by_asin.get(asin_up, {}) if doh_by_asin else {}
        row['soh_unit'] = live.get('soh_unit', 0) or 0
        row['soh_ltr'] = live.get('soh_ltr', 0) or 0
        row['drr_unit'] = live.get('drr_unit', 0) or 0
        row['drr_ltr'] = live.get('drr_ltr', 0) or 0
        row['doh'] = live.get('doh', 0) or 0
        bucket, score, reason = _compute_priority(
            row['drr_unit'], row['soh_unit'], row['doh'],
            row.get('days_to_expiry'), row.get('po_status'),
        )
        row['priority_bucket'] = bucket
        row['priority_score'] = score
        row['priority_reason'] = reason
        row['is_switch'] = bool(
            fc_pref
            and str(row.get('fulfillment_center') or '').strip().upper() != fc_pref
        )
        pool.append(row)
    return pool, doh_meta


def _reset_for_second_pass(item):
    """Strip a first-pass verdict off a line before it is re-planned unbacked.

    Every one of these was written by the pass that REFUSED the line — its cap,
    its reason, its zeroed quantities. Leaving any of them on would let the
    refusal outlive the decision to ignore stock, which is the exact thing the
    second pass exists to overturn.
    """
    for key in _SUGGESTION_ADD_STALE_KEYS:
        item.pop(key, None)
    for key in ('suggestion', 'suggestion_kind', 'short_reason'):
        item.pop(key, None)


class V2FillView(_SafeAPIView):
    """Build a truck from the channel's open book. Read-only — nothing is saved.

    POST body::

        {
          "channel": "CORE",
          "appointment_id": "576126037970",      // optional; sets the home FC
          "capacity_liters": 15000,
          "strategies": ["doh", "with_stock", "focus"],
          "families": ["MUSTARD"],               // focus only
          "asins": ["B0..."],                    // focus only, narrows packs
          "priority": {"premium": 60, "commodity": 40, "other": 0},
          "selected_pos": ["1ABCD234"]           // optional pool restriction
        }

    Returns the loaded lines, the lines left off WITH the reason each was left
    off, and the per-PO rollup the screen groups by.
    """

    permission_classes = [IsAuthenticated]

    def post(self, request):
        data = request.data if isinstance(request.data, dict) else {}
        channel = _norm_channel(data.get('channel'))
        appointment_id = str(data.get('appointment_id') or '').strip()
        strategies = _normalise_strategies(data.get('strategies'))
        selected_pos = data.get('selected_pos') or []
        families = _normalise_families(','.join(
            str(f) for f in (data.get('families') or [])
        ))
        asins = [str(a).strip().upper() for a in (data.get('asins') or []) if str(a).strip()]
        priority = _normalise_priority(data.get('priority')) if 'priority' in strategies else None

        capacity = _num(data.get('capacity_liters'), 0.0)
        if capacity <= 0:
            capacity = 15000.0
        capacity = max(V2_MIN_CAPACITY, min(V2_MAX_CAPACITY, capacity))

        # Focus with no family is not a focused truck, it is an unfiltered one
        # under a misleading heading. Say so rather than planning the whole
        # channel and labelling it "mustard".
        focused = 'focus' in strategies
        if focused and not families and not asins:
            return Response({
                'error': 'Product-focused filling needs at least one product family.',
                'families_available': list(PRODUCT_FAMILIES),
            }, status=400)

        with_stock = 'with_stock' in strategies
        without_stock = 'without_stock' in strategies
        # Neither picked is the safe reading of "just fill it": cap to live
        # stock, which is what every other planner surface does by default.
        if not with_stock and not without_stock:
            with_stock = True

        _pos, appt_fc = _appointment_pos(appointment_id)
        items, doh_meta = _fill_candidates(
            channel, appt_fc, selected_pos,
            families if focused else None,
            asins if focused else None,
        )
        if not items:
            return Response(self._empty(capacity, strategies, channel, appt_fc, doh_meta))

        order_key = _doh_fill_sort_key if 'doh' in strategies else _fill_sort_key
        items.sort(key=order_key)

        stock_detail = _planner_stock_detail()
        reserved = _reserved_stock_by_asin()
        avail_total = {
            a: max(0.0, d['onhand'] - reserved.get(a, 0.0))
            for a, d in stock_detail.items()
        }
        avail_remaining = dict(avail_total)

        # PASS 1. Stock caps are applied whenever stock is respected at all,
        # which includes the "both" case: filling from stock first is what stops
        # an unbacked line taking a slot from one that could leave today.
        pass1_unbacked = without_stock and not with_stock
        _apply_stock_caps(
            items, avail_total, avail_remaining, True, stock_detail, reserved,
            enforce_expiry=True, allow_unbacked=pass1_unbacked,
            min_units=MIN_AUTO_LINE_UNITS,
        )
        # Re-sort on what can actually ship. The order above ran on ORDERED
        # quantities because that is all there was — stock is allocated in list
        # order, so the pool has to be drained before anyone knows how much of
        # each line is real. Without this the plan reads 22, 22, 40, 783 while
        # claiming to be biggest-first.
        items.sort(key=order_key)

        # Even share across the chosen products. Packs beat families as the unit
        # of the split: picking MUSTARD 5L and SUNFLOWER 5L asks for half a truck
        # of each of THOSE, not of every mustard and every sunflower on the sheet.
        split_key = None
        if focused and (asins or len(families) > 1):
            asin_set = set(asins)
            split_key = lambda it: _even_share_key(it, families, asin_set)  # noqa: E731

        # `truck_size` is None throughout: 2.0's picker is a capacity in litres,
        # not one of the two legacy size keys, and the override wins anyway.
        loaded, not_loaded, capacity, planned_liters, load_pct, priority_actual = _auto_plan_truck(
            items, None, capacity_override=capacity, priority=priority,
            min_units=MIN_AUTO_LINE_UNITS, even_split_key=split_key,
        )
        # The packer returns the even-share report through the same slot as the
        # priority report; they are mutually exclusive, so unwrap it here.
        product_split = None
        if isinstance(priority_actual, dict) and 'product_split' in priority_actual:
            product_split = priority_actual.get('product_split')
            priority_actual = None

        # PASS 2 — the unbacked top-up. Only lines STOCK refused, only the
        # capacity pass 1 left behind. A line held back by expiry, by the
        # minimum-line floor on its ordered quantity, or by a full truck is not
        # re-offered: none of those are stock problems, and "plan without stock"
        # is not a licence to overrule the rest of the planner.
        unbacked_count = 0
        if with_stock and without_stock and not_loaded:
            remaining = max(0.0, capacity - planned_liters)
            retry = [
                it for it in not_loaded
                if (it.get('stock_limited') or it.get('stock_unfit')
                    or it.get('min_units_cause') == 'stock')
                and not it.get('expiry_blocked')
            ]
            if remaining > 0 and retry:
                # Identity, not equality: two lines of the same PO/ASIN can carry
                # equal dicts, and `not in` would drop the wrong one.
                retry_ids = {id(it) for it in retry}
                keep = [it for it in not_loaded if id(it) not in retry_ids]
                for it in retry:
                    _reset_for_second_pass(it)
                _apply_stock_caps(
                    retry, avail_total, avail_remaining, True, stock_detail, reserved,
                    enforce_expiry=True, allow_unbacked=True,
                    min_units=MIN_AUTO_LINE_UNITS,
                )
                retry.sort(key=order_key)
                add_loaded, add_not_loaded, add_used = _pack_into_capacity(
                    retry, remaining, True, MIN_AUTO_LINE_UNITS,
                )
                for it in add_loaded:
                    it['_unbacked_fill'] = True
                    it['fill_reason'] = (
                        'Planned without stock cover — the warehouse cannot back '
                        'this quantity today, so it has to arrive before dispatch.'
                    )
                unbacked_count = len(add_loaded)
                loaded.extend(add_loaded)
                not_loaded = keep + add_not_loaded
                planned_liters = round(planned_liters + float(add_used), 4)
                load_pct = round((planned_liters / capacity * 100) if capacity else 0, 2)

        loaded.sort(key=order_key)
        _tag_expiry_warnings(loaded)
        _tag_expiry_warnings(not_loaded)
        for it in not_loaded:
            it['planned_qty'] = 0
            it['planned_liters'] = 0
            it['not_loaded'] = True

        return Response({
            'loaded': [_serialize_row(it) for it in loaded],
            'not_loaded': [_serialize_row(it) for it in not_loaded],
            'by_po': self._by_po(loaded),
            'summary': self._summary(loaded, capacity, planned_liters, load_pct, unbacked_count),
            'priority_actual': priority_actual,
            'product_split': product_split,
            'strategies': strategies,
            'channel': channel,
            'appointment_id': appointment_id,
            'appointment_fc': appt_fc,
            'capacity_liters': capacity,
            'min_auto_line_units': MIN_AUTO_LINE_UNITS,
            'stock_meta': _stock_meta_payload(stock_detail),
            'doh_meta': doh_meta,
        })

    # ── shaping ──────────────────────────────────────────────────────────────

    @staticmethod
    def _summary(loaded, capacity, planned_liters, load_pct, unbacked_count):
        units = sum(_num(it.get('planned_qty')) for it in loaded)
        cartons = sum(
            _num(it.get('planned_qty')) / max(1.0, _num(it.get('case_pack'), 1.0))
            for it in loaded
        )
        return {
            'line_count': len(loaded),
            'po_count': len({
                str(it.get('po_number') or '').upper()
                for it in loaded if it.get('po_number')
            }),
            'sku_count': len({
                str(it.get('asin') or '').upper()
                for it in loaded if it.get('asin')
            }),
            'planned_units': int(round(units)),
            'planned_cartons': int(round(cartons)),
            'planned_liters': round(planned_liters, 2),
            'planned_tonnes': round(planned_liters / 1000.0, 3),
            'capacity_liters': round(capacity, 2),
            'load_pct': load_pct,
            'switch_count': sum(1 for it in loaded if it.get('is_switch')),
            'unbacked_count': unbacked_count,
        }

    @staticmethod
    def _by_po(loaded):
        """The plan rolled up PO-wise, in the order the truck was filled.

        A PO's position is its FIRST line's position, so the rollup reads in the
        same sequence as the manifest instead of re-sorting into its own order.
        """
        order, groups = [], {}
        for it in loaded:
            key = str(it.get('po_number') or '').strip().upper()
            g = groups.get(key)
            if g is None:
                g = {
                    'po_number': it.get('po_number'),
                    'fulfillment_center': it.get('fulfillment_center'),
                    'is_switch': bool(it.get('is_switch')),
                    'expiry_date': it.get('expiry_date'),
                    'line_count': 0,
                    'planned_qty': 0.0,
                    'planned_liters': 0.0,
                    'planned_cartons': 0.0,
                }
                groups[key] = g
                order.append(key)
            g['line_count'] += 1
            g['planned_qty'] += _num(it.get('planned_qty'))
            g['planned_liters'] += _num(it.get('planned_liters'))
            g['planned_cartons'] += (
                _num(it.get('planned_qty')) / max(1.0, _num(it.get('case_pack'), 1.0))
            )
        out = []
        for key in order:
            g = groups[key]
            g['planned_qty'] = int(round(g['planned_qty']))
            g['planned_liters'] = round(g['planned_liters'], 2)
            g['planned_cartons'] = int(round(g['planned_cartons']))
            out.append(_serialize_row(g))
        return out

    @staticmethod
    def _empty(capacity, strategies, channel, appt_fc, doh_meta):
        return {
            'loaded': [],
            'not_loaded': [],
            'by_po': [],
            'summary': {
                'line_count': 0, 'po_count': 0, 'sku_count': 0,
                'planned_units': 0, 'planned_cartons': 0,
                'planned_liters': 0.0, 'planned_tonnes': 0.0,
                'capacity_liters': round(capacity, 2), 'load_pct': 0.0,
                'switch_count': 0, 'unbacked_count': 0,
            },
            'priority_actual': None,
            'product_split': None,
            'strategies': strategies,
            'channel': channel,
            'appointment_fc': appt_fc,
            'capacity_liters': capacity,
            'min_auto_line_units': MIN_AUTO_LINE_UNITS,
            'empty_reason': (
                'Nothing here can ship today — every candidate line is billed, '
                'cancelled, locked to another shipment, or has no per-litre '
                'value on record.'
            ),
            'doh_meta': doh_meta,
        }


# ─────────────────────────────────────────────────────────────────────────────
# 5. Options for the Express panel
# ─────────────────────────────────────────────────────────────────────────────

class V2FillOptionsView(_SafeAPIView):
    """What the Express panel can offer for one channel.

    Only families that could actually FILL something are returned, counted
    through the planner's own gates — offering a family that plans an empty
    truck is worse than not offering it, because the empty result reads as a bug
    rather than as an answer. The item-head figures are there for the same
    reason: a priority slider that offers 40% Premium on a channel with no
    premium open is promising capacity nothing can fill.
    """

    permission_classes = [IsAuthenticated]

    def get(self, request):
        channel = _norm_channel(request.query_params.get('channel'))
        appointment_id = str(request.query_params.get('appointment_id') or '').strip()
        _pos, appt_fc = _appointment_pos(appointment_id)

        # One pass over the families rather than one query each: PRODUCT_FAMILIES
        # is a dozen names and a dozen round trips is a dozen round trips.
        #
        # Counted through the SAME pool the fill draws from (_POOL_*) — locked
        # lines, dispatched units and unpriced SKUs are all already gone, so the
        # litres on a family chip are litres the truck can genuinely reach for.
        fam_rows = {}
        fam_sql, fam_params = _family_sql(PRODUCT_FAMILIES)
        if fam_sql:
            where = list(_POOL_WHERE) + [fam_sql]
            params = list(fam_params)
            if channel:
                where.append(f"{_PO_CHANNEL} = %s")
                params.append(channel)
            # A line's family is its category when that IS a family, else the
            # family its sub-category names — the same two-sided test _family_sql
            # filters with, so nothing can pass the filter and then fail to land
            # in a bucket. Interpolated, not parameterised: the values come from
            # PRODUCT_FAMILIES, a module constant of bare uppercase words, never
            # from the request.
            case_arms = ' '.join(
                f"WHEN UPPER(TRIM(COALESCE(p.category, ''))) = '{f}'"
                f" OR STRPOS(UPPER(TRIM(COALESCE(p.sub_category, ''))), '{f}') > 0"
                f" THEN '{f}'"
                for f in PRODUCT_FAMILIES
            )
            with transaction.atomic():
                with connection.cursor() as cur:
                    _no_jit(cur)
                    cur.execute(f"""
                        {_POOL_CTES}
                        SELECT CASE {case_arms} ELSE NULL END        AS family,
                               COUNT(DISTINCT UPPER(TRIM(p.po_number))) AS po_count,
                               COUNT(DISTINCT UPPER(TRIM(p.asin)))      AS sku_count,
                               COALESCE(SUM({_POOL_UNITS}), 0)          AS units,
                               COALESCE(SUM({_POOL_UNITS} * p.per_liter), 0) AS liters
                        FROM reporting."Amazon PO" p
                        {_POOL_JOINS}
                        WHERE {' AND '.join(where)}
                        GROUP BY 1
                    """, params)
                    fam_rows = {
                        r['family']: r
                        for r in _row_to_dict(cur, cur.fetchall())
                        if r['family']
                    }

        families = [
            _serialize_row({
                'family': name,
                'po_count': int(fam_rows[name]['po_count'] or 0),
                'sku_count': int(fam_rows[name]['sku_count'] or 0),
                'units': _num(fam_rows[name]['units']),
                'liters': _num(fam_rows[name]['liters']),
            })
            for name in PRODUCT_FAMILIES
            if name in fam_rows and int(fam_rows[name]['po_count'] or 0) > 0
        ]

        # Item heads the priority split could actually fill. Same pool as the
        # families above and for the same reason: a slider offering 40% Premium
        # on a channel whose premium lines are all locked elsewhere is promising
        # capacity nothing can take.
        head_where = list(_POOL_WHERE)
        head_params: list = []
        if channel:
            head_where.append(f"{_PO_CHANNEL} = %s")
            head_params.append(channel)
        with transaction.atomic():
            with connection.cursor() as cur:
                _no_jit(cur)
                cur.execute(f"""
                    {_POOL_CTES}
                    SELECT {_item_head_case('p.item_head')}       AS bucket,
                           COUNT(*)                               AS line_count,
                           COUNT(DISTINCT UPPER(TRIM(p.po_number))) AS po_count,
                           COALESCE(SUM({_POOL_UNITS}), 0)          AS units,
                           COALESCE(SUM({_POOL_UNITS} * p.per_liter), 0) AS liters
                    FROM reporting."Amazon PO" p
                    {_POOL_JOINS}
                    WHERE {' AND '.join(head_where)}
                    GROUP BY 1
                """, head_params)
                buckets = {r['bucket']: r for r in _row_to_dict(cur, cur.fetchall())}

        return Response({
            'channel': channel,
            'appointment_fc': appt_fc,
            'families': families,
            'item_heads': [
                _serialize_row({
                    'bucket': name,
                    'line_count': int((buckets.get(name) or {}).get('line_count') or 0),
                    'po_count': int((buckets.get(name) or {}).get('po_count') or 0),
                    'units': _num((buckets.get(name) or {}).get('units')),
                    'liters': _num((buckets.get(name) or {}).get('liters')),
                })
                for name in ('PREMIUM', 'COMMODITY', 'OTHER')
            ],
            'truck_presets': [
                {'liters': c, 'tonnes': c / 1000.0} for c in V2_TRUCK_PRESETS
            ],
            'capacity_range': {'min': V2_MIN_CAPACITY, 'max': V2_MAX_CAPACITY},
            'strategies': list(V2_STRATEGIES),
        })
