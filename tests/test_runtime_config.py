"""Offline configuration checks; no real SQLAlchemy, database, or Telegram calls."""

import ast
import importlib.util
import os
from pathlib import Path
import sys
import types
import unittest
from unittest.mock import Mock, patch
from urllib.parse import unquote, urlsplit

from core.runtime_config import ConfigurationError, get_database_url, required_env

ROOT = Path(__file__).resolve().parents[1]


def load_isolated(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class RuntimeConfigTests(unittest.TestCase):
    def setUp(self):
        self.env = patch.dict(os.environ, {}, clear=True)
        self.env.start()
        self.legacy = patch.dict(sys.modules, {"mysql_config": None})
        self.legacy.start()
        self.addCleanup(self.env.stop)
        self.addCleanup(self.legacy.stop)

    def postgres_settings(self):
        os.environ.update(DB_USER="test-user", DB_PASS="synthetic-pass",
                          DB_HOST="localhost", DB_NAME="test-db")

    def test_required_environment_reports_only_name(self):
        for value in (None, "", " "):
            with self.subTest(value=value):
                if value is None:
                    os.environ.pop("TELEGRAM_BOT_TOKEN", None)
                else:
                    os.environ["TELEGRAM_BOT_TOKEN"] = value
                with self.assertRaisesRegex(ConfigurationError, "TELEGRAM_BOT_TOKEN"):
                    required_env("TELEGRAM_BOT_TOKEN")

    def test_explicit_url_precedes_legacy_configuration(self):
        os.environ["DATABASE_URL"] = "sqlite+aiosqlite:///:memory:"
        with patch("core.runtime_config.importlib.import_module") as importer:
            self.assertEqual(get_database_url(), os.environ["DATABASE_URL"])
            importer.assert_not_called()

    def test_legacy_mysql_configuration_is_preserved(self):
        legacy = types.ModuleType("mysql_config")
        legacy.MYSQL_URL = "mysql+aiomysql://test-user:synthetic-pass@localhost/test-db"
        with patch.dict(sys.modules, {"mysql_config": legacy}):
            self.assertEqual(get_database_url(), legacy.MYSQL_URL)

    def test_invalid_legacy_configuration_does_not_fall_back(self):
        self.postgres_settings()
        with patch.dict(sys.modules, {"mysql_config": types.ModuleType("mysql_config")}):
            with self.assertRaisesRegex(ConfigurationError, "mysql_config.MYSQL_URL"):
                get_database_url()

    def test_broken_legacy_dependency_has_safe_error(self):
        failure = ModuleNotFoundError("synthetic-sensitive-detail", name="missing_driver")
        with patch("core.runtime_config.importlib.import_module", side_effect=failure):
            with self.assertRaises(ConfigurationError) as caught:
                get_database_url()
        self.assertNotIn("synthetic-sensitive-detail", str(caught.exception))

    def test_postgres_fields_escape_reserved_characters(self):
        self.postgres_settings()
        os.environ.update(DB_USER="test@user", DB_PASS="synthetic:/@ %", DB_NAME="test/db")
        parts = urlsplit(get_database_url())
        self.assertEqual(parts.scheme, "postgresql+asyncpg")
        self.assertEqual(unquote(parts.username), os.environ["DB_USER"])
        self.assertEqual(unquote(parts.password), os.environ["DB_PASS"])
        self.assertEqual(unquote(parts.path[1:]), os.environ["DB_NAME"])
        self.assertEqual(parts.port, 5432)

    def test_each_postgres_field_is_required(self):
        for name in ("DB_USER", "DB_PASS", "DB_HOST", "DB_NAME"):
            with self.subTest(name=name):
                self.postgres_settings()
                del os.environ[name]
                with self.assertRaisesRegex(ConfigurationError, name):
                    get_database_url()

    def test_port_and_host_validation_excludes_values(self):
        self.postgres_settings()
        for value in ("synthetic-invalid-port", "0", "65536"):
            with self.subTest(port=value):
                os.environ["DB_PORT"] = value
                with self.assertRaises(ConfigurationError) as caught:
                    get_database_url()
                self.assertIn("DB_PORT", str(caught.exception))
                self.assertNotIn("synthetic-invalid-port", str(caught.exception))
        os.environ["DB_PORT"] = "5432"
        os.environ["DB_HOST"] = "synthetic-host/invalid"
        with self.assertRaisesRegex(ConfigurationError, "DB_HOST") as caught:
            get_database_url()
        self.assertNotIn(os.environ["DB_HOST"], str(caught.exception))

    def test_ipv6_host_is_bracketed(self):
        self.postgres_settings()
        os.environ["DB_HOST"] = "::1"
        self.assertEqual(urlsplit(get_database_url()).hostname, "::1")

    def test_database_entrypoints_use_shared_configuration(self):
        for filename in ("db_engine.py", "db_create_tables.py"):
            module = ast.parse((ROOT / "core" / filename).read_text(encoding="utf-8"))
            assignments = [node for node in module.body if isinstance(node, ast.Assign)
                           and any(isinstance(target, ast.Name) and target.id == "DATABASE_URL"
                                   for target in node.targets)]
            self.assertEqual(len(assignments), 1)
            value = assignments[0].value
            self.assertIsInstance(value, ast.Call)
            self.assertEqual(value.func.id, "get_database_url")

    def test_engine_import_uses_mocked_sqlalchemy_only(self):
        os.environ["DATABASE_URL"] = "sqlite+aiosqlite:///:memory:"
        async_api = types.ModuleType("sqlalchemy.ext.asyncio")
        async_api.create_async_engine = Mock(return_value=object())
        async_api.async_sessionmaker = Mock(return_value=object())
        async_api.AsyncSession = type("AsyncSession", (), {})
        with patch.dict(sys.modules, {"sqlalchemy.ext.asyncio": async_api}):
            load_isolated("offline_db_engine", ROOT / "core" / "db_engine.py")
        self.assertEqual(async_api.create_async_engine.call_args.args, (os.environ["DATABASE_URL"],))

    def test_telegram_configuration_and_failure_logs_are_safe(self):
        os.environ.update(TELEGRAM_BOT_TOKEN="unit-test-token", TELEGRAM_CHAT_ID="unit-test-chat")
        requests = types.ModuleType("requests")
        requests.post = Mock(side_effect=RuntimeError("unit-test-token must stay private"))
        with patch.dict(sys.modules, {"requests": requests}):
            notifier = load_isolated("offline_notifier", ROOT / "core" / "telegram_notify.py")
            self.assertEqual(notifier.TELEGRAM_BOT_TOKEN, "unit-test-token")
            self.assertEqual(notifier.TELEGRAM_CHAT_ID, "unit-test-chat")
            with self.assertLogs(level="ERROR") as logs:
                self.assertFalse(notifier.send_telegram_message("synthetic-message"))
            self.assertNotIn("unit-test-token", " ".join(logs.output))
            requests.post.side_effect = None
            requests.post.return_value = types.SimpleNamespace(status_code=400, text="private-response")
            with self.assertLogs(level="ERROR") as logs:
                self.assertFalse(notifier.send_telegram_message("synthetic-message"))
            self.assertNotIn("private-response", " ".join(logs.output))

    def test_notifier_requires_each_setting_before_any_request(self):
        for name in ("TELEGRAM_BOT_TOKEN", "TELEGRAM_CHAT_ID"):
            with self.subTest(name=name):
                os.environ.update(TELEGRAM_BOT_TOKEN="unit-test-token", TELEGRAM_CHAT_ID="unit-test-chat")
                del os.environ[name]
                requests = types.ModuleType("requests")
                requests.post = Mock()
                with patch.dict(sys.modules, {"requests": requests}):
                    with self.assertRaisesRegex(ConfigurationError, name):
                        load_isolated("offline_missing_notifier", ROOT / "core" / "telegram_notify.py")
                requests.post.assert_not_called()


if __name__ == "__main__":
    unittest.main()
