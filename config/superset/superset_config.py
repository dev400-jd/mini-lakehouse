import os

from redis import Redis
from flask_caching.backends.rediscache import RedisCache


def env(name: str, default: str | None = None) -> str:
    value = os.getenv(name, default)

    if value is None:
        raise RuntimeError(f"Required environment variable {name} is missing")

    return value


# ---------------------------------------------------------------------------
# Grundkonfiguration
# ---------------------------------------------------------------------------

SECRET_KEY = env("SUPERSET_SECRET_KEY")

SQLALCHEMY_DATABASE_URI = (
    "postgresql+psycopg2://"
    f"{env('SUPERSET_DB_USER')}:"
    f"{env('SUPERSET_DB_PASSWORD')}@"
    f"{env('SUPERSET_DB_HOST', 'postgres')}:"
    f"{env('SUPERSET_DB_PORT', '5432')}/"
    f"{env('SUPERSET_DB_NAME', 'superset')}"
)

# Verhindert unnötigen SQLAlchemy-Verbindungs-Overhead.
SQLALCHEMY_TRACK_MODIFICATIONS = False

# Verbindungen vor Wiederverwendung prüfen.
SQLALCHEMY_ENGINE_OPTIONS = {
    "pool_pre_ping": True,
    "pool_recycle": 300,
}


# ---------------------------------------------------------------------------
# Redis-Caching
# ---------------------------------------------------------------------------

REDIS_HOST = env("REDIS_HOST", "redis")
REDIS_PORT = int(env("REDIS_PORT", "6379"))

CACHE_CONFIG = {
    "CACHE_TYPE": "RedisCache",
    "CACHE_DEFAULT_TIMEOUT": 300,
    "CACHE_KEY_PREFIX": "superset_metadata_",
    "CACHE_REDIS_HOST": REDIS_HOST,
    "CACHE_REDIS_PORT": REDIS_PORT,
    "CACHE_REDIS_DB": 1,
}

DATA_CACHE_CONFIG = {
    "CACHE_TYPE": "RedisCache",
    "CACHE_DEFAULT_TIMEOUT": 300,
    "CACHE_KEY_PREFIX": "superset_data_",
    "CACHE_REDIS_HOST": REDIS_HOST,
    "CACHE_REDIS_PORT": REDIS_PORT,
    "CACHE_REDIS_DB": 2,
}

FILTER_STATE_CACHE_CONFIG = {
    "CACHE_TYPE": "RedisCache",
    "CACHE_DEFAULT_TIMEOUT": 86400,
    "CACHE_KEY_PREFIX": "superset_filter_state_",
    "CACHE_REDIS_HOST": REDIS_HOST,
    "CACHE_REDIS_PORT": REDIS_PORT,
    "CACHE_REDIS_DB": 3,
}

EXPLORE_FORM_DATA_CACHE_CONFIG = {
    "CACHE_TYPE": "RedisCache",
    "CACHE_DEFAULT_TIMEOUT": 86400,
    "CACHE_KEY_PREFIX": "superset_explore_",
    "CACHE_REDIS_HOST": REDIS_HOST,
    "CACHE_REDIS_PORT": REDIS_PORT,
    "CACHE_REDIS_DB": 4,
}


# ---------------------------------------------------------------------------
# Lokale Entwicklungs-/Laborumgebung
# ---------------------------------------------------------------------------

WTF_CSRF_ENABLED = True
SESSION_COOKIE_HTTPONLY = True
SESSION_COOKIE_SAMESITE = "Lax"

# Erst hinter einem HTTPS-Reverse-Proxy auf True setzen.
SESSION_COOKIE_SECURE = False

# Für Docker-internes HTTP zunächst deaktiviert.
TALISMAN_ENABLED = False

# Optional: Sprache und Zeitzone
# BABEL_DEFAULT_LOCALE = "en"
SUPERSET_WEBSERVER_TIMEOUT = 120

# Unterstützt längere Trino-Abfragen über den Webserver.
SQLLAB_TIMEOUT = 120