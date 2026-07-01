from datetime import timedelta
from pathlib import Path

import environ

BASE_DIR = Path(__file__).resolve().parent.parent.parent

env = environ.Env(
    DJANGO_DEBUG=(bool, False),
    FIREBASE_NOTIFICATIONS_ENABLED=(bool, False),
)
environ.Env.read_env(BASE_DIR / ".env")


def _clean_env_list(name, default):
    return [value.strip() for value in env.list(name, default=default) if value.strip()]


SECRET_KEY = env("DJANGO_SECRET_KEY", default="insecure-dev-key")
DEBUG = env.bool("DJANGO_DEBUG", default=False)
ALLOWED_HOSTS = _clean_env_list(
    "DJANGO_ALLOWED_HOSTS",
    default=["localhost", "127.0.0.1"],
)

INSTALLED_APPS = [
    "django.contrib.admin",
    "django.contrib.auth",
    "django.contrib.contenttypes",
    "django.contrib.sessions",
    "django.contrib.messages",
    "django.contrib.staticfiles",
    "rest_framework",
    "corsheaders",
    "django_filters",
    "accounts",
    "platforms",
    "warehouse",
    "sap",
    "dashboard",
    "uploads",
    "shipment",
]

MIDDLEWARE = [
    "corsheaders.middleware.CorsMiddleware",
    "django.middleware.security.SecurityMiddleware",
    "django.contrib.sessions.middleware.SessionMiddleware",
    "django.middleware.common.CommonMiddleware",
    "django.middleware.csrf.CsrfViewMiddleware",
    "django.contrib.auth.middleware.AuthenticationMiddleware",
    "django.contrib.messages.middleware.MessageMiddleware",
    "django.middleware.clickjacking.XFrameOptionsMiddleware",
]

ROOT_URLCONF = "config.urls"
WSGI_APPLICATION = "config.wsgi.application"

TEMPLATES = [
    {
        "BACKEND": "django.template.backends.django.DjangoTemplates",
        "DIRS": [],
        "APP_DIRS": True,
        "OPTIONS": {
            "context_processors": [
                "django.template.context_processors.request",
                "django.contrib.auth.context_processors.auth",
                "django.contrib.messages.context_processors.messages",
            ],
        },
    },
]

DATABASES = {
    "default": {
        "ENGINE": "django.db.backends.postgresql",
        "NAME": env("POSTGRES_DB", default="ecms"),
        "USER": env("POSTGRES_USER", default="ecms"),
        "PASSWORD": env("POSTGRES_PASSWORD", default="ecms"),
        "HOST": env("POSTGRES_HOST", default="127.0.0.1"),
        "PORT": env("POSTGRES_PORT", default="5432"),
        # Reuse a connection across requests instead of opening one per request.
        # Big latency win on every API call; safe with WSGI + per-worker pooling.
        "CONN_MAX_AGE": env.int("DJANGO_DB_CONN_MAX_AGE", default=600),
        "CONN_HEALTH_CHECKS": True,
        # Per-connection work_mem. The dashboard's master_po view runs several
        # large sorts/dedupes over ~41k rows; at Postgres' 4MB default they
        # spill to disk ("Sort Method: external merge Disk" in EXPLAIN), which
        # is slow. Giving each query more sort memory keeps those sorts in RAM.
        # This is read-only tuning: it changes no data, no query, and no result
        # — only how much memory a single sort/hash step may use. Tune or revert
        # per environment with DJANGO_DB_WORK_MEM (e.g. "32MB", or "4MB" to
        # restore the Postgres default).
        "OPTIONS": {
            "options": f"-c work_mem={env('DJANGO_DB_WORK_MEM', default='64MB')}",
            # Cap the initial connect handshake so a stalled/overloaded DB fails
            # fast instead of hanging the worker (and piling up requests) on the
            # OS default (~a couple of minutes). Tune via DJANGO_DB_CONNECT_TIMEOUT.
            "connect_timeout": env.int("DJANGO_DB_CONNECT_TIMEOUT", default=5),
        },
    },
}

# Cache backend.
#
# Default is per-process LocMemCache (no infra, fully safe). Its weakness in
# production is that it is NOT shared across gunicorn workers: every worker keeps
# its own copy, so a request landing on a "cold" worker misses the cache and
# re-runs the heavy dashboard SQL — the main cause of intermittent 1-2s loads.
#
# Set REDIS_URL (e.g. "redis://127.0.0.1:6379/1") to switch to a SHARED cache so
# the first worker to compute a dashboard payload warms it for ALL workers (and,
# combined with `shared=True` on user-independent endpoints, all users). This is
# the single highest-leverage latency fix and requires only a running Redis +
# the `redis` package (already in requirements). Defaults preserve current
# behaviour exactly when REDIS_URL is unset.
REDIS_URL = env("REDIS_URL", default="")
if REDIS_URL:
    CACHES = {
        "default": {
            "BACKEND": "django.core.cache.backends.redis.RedisCache",
            "LOCATION": REDIS_URL,
            "TIMEOUT": 300,
            "KEY_PREFIX": "ecms",
        },
    }
else:
    CACHES = {
        "default": {
            "BACKEND": "django.core.cache.backends.locmem.LocMemCache",
            "LOCATION": "ecms-default",
            "TIMEOUT": 300,
            "OPTIONS": {"MAX_ENTRIES": 5000},
        },
    }

# SAP HANA credentials are consumed by sap/service.py via hdbcli,
# not as a Django database connection.
HANA = {
    "host": env("HANA_HOST", default=""),
    "port": env.int("HANA_PORT", default=30015),
    "user": env("HANA_USER", default=""),
    "password": env("HANA_PASSWORD", default=""),
    "schema": env("HANA_SCHEMA", default=""),
    # Fail fast when HANA is unreachable (VPN down / host blocked) instead of
    # hanging on the OS default (~20-30s) and freezing every SAP-backed widget.
    "connect_timeout_ms": env.int("HANA_CONNECT_TIMEOUT_MS", default=5000),
    # Per-statement timeout (seconds). connect_timeout only covers the initial
    # handshake; once connected, a slow query/proc would otherwise block the
    # gunicorn worker indefinitely (the failure mode behind the dashboard
    # all-zeros incident). This caps any single SAP query so a worker is freed.
    # 0 disables. Tune via HANA_QUERY_TIMEOUT_S.
    "query_timeout_s": env.int("HANA_QUERY_TIMEOUT_S", default=30),
}

FIREBASE_CREDENTIALS_FILE = env("FIREBASE_CREDENTIALS_FILE", default="")
FIREBASE_NOTIFICATIONS_ENABLED = env.bool("FIREBASE_NOTIFICATIONS_ENABLED", default=False)
FIREBASE_DOH_TOPIC = env("FIREBASE_DOH_TOPIC", default="inventory_doh_alerts")

# Google Sheets (read-only) integration. The credentials file is a service
# account JSON key; share the target spreadsheet with its client_email as Viewer.
GOOGLE_SHEETS_CREDENTIALS_FILE = env(
    "GOOGLE_SHEETS_CREDENTIALS_FILE",
    default=str(BASE_DIR / "secrets" / "google_sheets_credentials.json"),
)
GOOGLE_SHEETS_SPREADSHEET_ID = env(
    "GOOGLE_SHEETS_SPREADSHEET_ID",
    default="10-P_ZBVGaIKz87PTByk8rMb_c1J0VT0mfZo_84qwjQU",
)

DATABASE_ROUTERS = ["sap.router.SAPReadOnlyRouter"]

AUTH_USER_MODEL = "accounts.User"

AUTH_PASSWORD_VALIDATORS = []

LANGUAGE_CODE = "en-us"
TIME_ZONE = "Asia/Kolkata"
USE_I18N = True
USE_TZ = True

STATIC_URL = "static/"
STATIC_ROOT = BASE_DIR / "staticfiles"
DEFAULT_AUTO_FIELD = "django.db.models.BigAutoField"

# Allow pasted CSV data up to 25 MB (default is 2.5 MB, too small for large Amazon PO pastes)
DATA_UPLOAD_MAX_MEMORY_SIZE = 25 * 1024 * 1024

REST_FRAMEWORK = {
    "DEFAULT_AUTHENTICATION_CLASSES": (
        "rest_framework_simplejwt.authentication.JWTAuthentication",
        "rest_framework.authentication.SessionAuthentication",
    ),
    "DEFAULT_PERMISSION_CLASSES": (
        "rest_framework.permissions.IsAuthenticated",
    ),
    "DEFAULT_FILTER_BACKENDS": (
        "django_filters.rest_framework.DjangoFilterBackend",
        "rest_framework.filters.SearchFilter",
        "rest_framework.filters.OrderingFilter",
    ),
    "DEFAULT_PAGINATION_CLASS": "rest_framework.pagination.PageNumberPagination",
    "PAGE_SIZE": 50,
}

SIMPLE_JWT = {
    "ACCESS_TOKEN_LIFETIME": timedelta(hours=24),
    # Long-lived, rotating refresh token: as long as the user opens the app
    # within this window the frontend silently renews the access token, so a
    # logged-in user is never auto-logged-out — only a manual logout ends the
    # session. Rotation issues a fresh refresh token on every renewal, which
    # keeps extending the window for anyone who keeps using the app.
    "REFRESH_TOKEN_LIFETIME": timedelta(days=365),
    "ROTATE_REFRESH_TOKENS": True,
    # We do NOT run the token_blacklist app, so rotation must not try to
    # blacklist the old refresh token (that would error on a missing table).
    # The old token simply stays valid until it expires on its own.
    "BLACKLIST_AFTER_ROTATION": False,
    "ALGORITHM": "HS256",
    "SIGNING_KEY": env("JWT_SIGNING_KEY", default=SECRET_KEY),
    "USER_ID_FIELD": "id",
    "USER_ID_CLAIM": "user_id",
}

CORS_ALLOWED_ORIGINS = _clean_env_list(
    "CORS_ALLOWED_ORIGINS",
    default=[
        "http://localhost:5173",
        "http://127.0.0.1:5173",
        "http://localhost:5174",
        "http://127.0.0.1:5174",
    ],
)
CORS_ALLOW_CREDENTIALS = True

# Shared secret for the unattended Amazon appointment-commit importer
# (Tampermonkey auto-run script POSTs carton/unit counts here). Scoped to the
# appointment-commit import endpoint only. Empty = endpoint disabled (safe
# default). Set APPOINTMENT_COMMIT_IMPORT_KEY in .env to enable.
APPOINTMENT_COMMIT_IMPORT_KEY = env("APPOINTMENT_COMMIT_IMPORT_KEY", default="")
