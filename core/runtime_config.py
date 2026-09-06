"""Runtime configuration without embedded credentials or network side effects."""

import importlib
import os
from urllib.parse import quote


class ConfigurationError(RuntimeError):
    """A required runtime setting is absent or invalid."""


def required_env(name: str) -> str:
    value = os.environ.get(name)
    if value is None or not value.strip():
        raise ConfigurationError(f"Required environment variable is missing: {name}")
    return value


def get_database_url() -> str:
    """Prefer an explicit URL, then legacy mysql_config, then PostgreSQL fields."""
    explicit_url = os.environ.get("DATABASE_URL", "")
    if explicit_url.strip():
        return explicit_url

    try:
        legacy = importlib.import_module("mysql_config")
    except ModuleNotFoundError as error:
        if error.name != "mysql_config":
            raise ConfigurationError("mysql_config has an unavailable dependency") from None
    except ImportError:
        raise ConfigurationError("mysql_config could not be imported") from None
    else:
        legacy_url = getattr(legacy, "MYSQL_URL", None)
        if not isinstance(legacy_url, str) or not legacy_url.strip():
            raise ConfigurationError("mysql_config.MYSQL_URL must be a non-empty connection URL")
        return legacy_url

    user = required_env("DB_USER")
    password = required_env("DB_PASS")
    host = required_env("DB_HOST")
    name = required_env("DB_NAME")
    port_text = os.environ.get("DB_PORT", "5432")
    try:
        port = int(port_text)
    except ValueError:
        raise ConfigurationError("DB_PORT must be an integer from 1 to 65535") from None
    if not 1 <= port <= 65535:
        raise ConfigurationError("DB_PORT must be an integer from 1 to 65535")
    if any(char in host for char in "/@?#") or any(char.isspace() for char in host):
        raise ConfigurationError("DB_HOST must be a hostname or IP address")
    if ":" in host and not host.startswith("["):
        host = f"[{host}]"
    return (
        f"postgresql+asyncpg://{quote(user, safe='')}:{quote(password, safe='')}"
        f"@{host}:{port}/{quote(name, safe='')}"
    )
