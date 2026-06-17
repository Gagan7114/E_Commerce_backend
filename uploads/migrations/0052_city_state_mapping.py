"""City -> State mapping table for the Secondary state-sales map.

Resolves secmaster_mv.location (city) to a canonical Indian state. Seeded from
Location_Master (clean city->state, ~99% of QC secondary qty) plus 63 confirmed
gap cities so coverage is ~100%. The match key (`city_key`) is the city upper-
cased with every non-alphanumeric run collapsed to a single space, so messy
inputs ("SRI_GANGANAGAR", "HAMIRPUR (HIMACHAL PRADESH)", "GURGAON-FARIDABAD")
match — the /state-sales secondary query normalises secmaster_mv.location the
same way before joining.
"""
import re

from django.db import migrations


def _ck(s):
    return re.sub(r"[^A-Z0-9]+", " ", str(s or "").upper()).strip()


# Location_Master state spellings that aren't canonical.
_STATE_FIX = {"CHATTISGARH": "CHHATTISGARH"}


def _nstate(s):
    u = str(s or "").strip().upper()
    if not u or u == "UNKNOWN":
        return None
    return _STATE_FIX.get(u, u)


# 63 cities present in secmaster_mv.location (QC platforms) but missing from
# Location_Master — states confirmed.
GAP_CITIES = [
    ("NEW CHANDIGARH", "CHANDIGARH"), ("NEWCHANDIGARH", "CHANDIGARH"),
    ("SRI_GANGANAGAR", "RAJASTHAN"), ("FARIDKOT", "PUNJAB"),
    ("SUNDAR_NAGAR", "HIMACHAL PRADESH"), ("HAMIRPUR (HIMACHAL PRADESH)", "HIMACHAL PRADESH"),
    ("RUPNAGAR", "PUNJAB"), ("BARNALA", "PUNJAB"),
    ("UNA (HIMACHAL PRADESH)", "HIMACHAL PRADESH"), ("KOLKATA RURAL", "WEST BENGAL"),
    ("AURANGABAD (MAHARASHTRA)", "MAHARASHTRA"), ("MUZZAFFARNAGAR", "UTTAR PRADESH"),
    ("KRISHNA DISTRICT", "ANDHRA PRADESH"), ("MUKTSAR", "PUNJAB"),
    ("NIZAMABAD", "TELANGANA"), ("BANGALORE RURAL", "KARNATAKA"),
    ("SATARA", "MAHARASHTRA"), ("RAMPUR (UTTAR PRADESH)", "UTTAR PRADESH"),
    ("SANGLI", "MAHARASHTRA"), ("ABOHAR", "PUNJAB"),
    ("TARN TARAN SAHIB", "PUNJAB"), ("VIKASNAGAR", "UTTARAKHAND"),
    ("GULBARGA", "KARNATAKA"), ("PUDUCHERRY", "PUDUCHERRY"),
    ("SOLAPUR", "MAHARASHTRA"), ("BHARATPUR", "RAJASTHAN"),
    ("PILIBHIT", "UTTAR PRADESH"), ("AMROHA", "UTTAR PRADESH"),
    ("CHIKKAMAGALURU", "KARNATAKA"), ("JALGAON", "MAHARASHTRA"),
    ("CHARKHI DADRI", "HARYANA"), ("DINDIGUL", "TAMIL NADU"),
    ("KHURJA", "UTTAR PRADESH"), ("BUDAUN", "UTTAR PRADESH"),
    ("HATHRAS", "UTTAR PRADESH"), ("KANGRA", "HIMACHAL PRADESH"),
    ("BIJAPUR (KARNATAKA)", "KARNATAKA"), ("SHIKOHABAD", "UTTAR PRADESH"),
    ("PURI", "ODISHA"), ("ETAH", "UTTAR PRADESH"),
    ("KHARAGPUR", "WEST BENGAL"), ("KARAD", "MAHARASHTRA"),
    ("SHILLONG", "MEGHALAYA"), ("BIDAR", "KARNATAKA"),
    ("HANSI", "HARYANA"), ("RAICHUR", "KARNATAKA"),
    ("MANDYA", "KARNATAKA"), ("CHITRADURGA", "KARNATAKA"),
    ("CUDDALORE", "TAMIL NADU"), ("GURGAON-FARIDABAD", "HARYANA"),
    ("BIJAPUR", "KARNATAKA"), ("BHUBANESWAR RURAL", "ODISHA"),
    ("SHAMLI", "UTTAR PRADESH"), ("BARAUT", "UTTAR PRADESH"),
    ("SIKAR", "RAJASTHAN"), ("JALNA", "MAHARASHTRA"),
    ("RAEBARELI", "UTTAR PRADESH"), ("JHUNJHUNU", "RAJASTHAN"),
    ("AKOLA", "MAHARASHTRA"), ("AYODHYA", "UTTAR PRADESH"),
    ("UDAIPUR (RAJASTHAN)", "RAJASTHAN"), ("RATNAGIRI", "MAHARASHTRA"),
    ("SITAPUR", "UTTAR PRADESH"),
]


def seed(apps, schema_editor):
    cur = schema_editor.connection.cursor()
    rows = {}  # city_key -> (display_city, state, source)
    # 1) Location_Master (clean city -> state).
    cur.execute(
        'SELECT "CITY", "STATE" FROM public."Location_Master" '
        "WHERE NULLIF(TRIM(\"CITY\"), '') IS NOT NULL"
    )
    for city, state in cur.fetchall():
        key = _ck(city)
        st = _nstate(state)
        if not key or not st:
            continue
        rows.setdefault(key, (str(city).strip().upper(), st, "location_master"))
    # 2) Confirmed gap cities (authoritative for their keys).
    for city, st in GAP_CITIES:
        rows[_ck(city)] = (city, st, "seed")
    for key, (city, st, src) in rows.items():
        cur.execute(
            "INSERT INTO public.city_state_mapping (city, city_key, state, source) "
            "VALUES (%s, %s, %s, %s) ON CONFLICT (city_key) DO NOTHING",
            [city, key, st, src],
        )


class Migration(migrations.Migration):
    dependencies = [
        ("uploads", "0051_amazon_sec_state"),
    ]

    operations = [
        migrations.RunSQL(
            sql=r"""
            CREATE TABLE IF NOT EXISTS public.city_state_mapping (
                id          bigserial PRIMARY KEY,
                city        text NOT NULL,
                city_key    text NOT NULL UNIQUE,
                state       text NOT NULL,
                source      text DEFAULT 'seed',
                updated_at  timestamp without time zone DEFAULT now()
            );
            CREATE INDEX IF NOT EXISTS idx_city_state_mapping_state
                ON public.city_state_mapping (state);
            """,
            reverse_sql="DROP TABLE IF EXISTS public.city_state_mapping;",
        ),
        migrations.RunPython(seed, migrations.RunPython.noop),
    ]
