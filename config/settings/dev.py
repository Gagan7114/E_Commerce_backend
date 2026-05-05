from .base import *  # noqa: F401,F403

DEBUG = True
INTERNAL_IPS = ["127.0.0.1"]
CORS_ALLOWED_ORIGIN_REGEXES = [
    r"^http://localhost:\d+$",
    r"^http://127\.0\.0\.1:\d+$",
]
