# Runtime credential migration

This change removes embedded notification and database credentials from the working source. Existing Git history may retain earlier values. Revocation/rotation and any history cleanup require a separate coordinated rollout.

## Before deploying

1. Record which running service uses this checkout and how its environment is supplied. Preserve its external configuration and access permissions.
2. Provision `TELEGRAM_BOT_TOKEN` and `TELEGRAM_CHAT_ID` for the same bot and intended recipient. Missing settings now raise a `ConfigurationError` naming only the variable. Notification exceptions record the exception type or HTTP status, excluding token-bearing URLs and response bodies.
3. Choose the database source, in precedence order:
   - `DATABASE_URL`: an explicit SQLAlchemy async connection URL;
   - the existing external `mysql_config.py` with `MYSQL_URL`: retained for compatibility;
   - `DB_USER`, `DB_PASS`, `DB_HOST`, `DB_NAME`, plus optional `DB_PORT` (default `5432`): PostgreSQL with the existing `asyncpg` dialect.
4. The PostgreSQL fallback now requires supplied credentials. Reserved characters in its user, password, and database name are URL-encoded. Invalid settings produce errors without printing values. Existing database drivers/dependencies remain unchanged.
5. `.env.example` is a template. These modules read the process environment; arrange loading through the service manager or your existing launcher. Copying a `.env` file alone does not inject it into the process.
6. Preserve distributor credentials and their existing configuration sources. This patch changes notification/database configuration only.

## Offline checks

```bash
python -m unittest discover -s tests -p "test_runtime_config.py"
```

The tests use synthetic values, stub the external configuration module and SQLAlchemy, and make no database/Telegram requests. `db_create_tables.py` is checked as source only: running it can modify or truncate database tables.

After the deployment environment is prepared, review the diff in a secret-redacting viewer, deploy through the existing workflow, then verify the same database and notification recipient. Coordinate provider credential rotation with service configuration. Keep real `.env`, `mysql_config.py`, session and private-key files outside Git. A source-only change does not revoke a previously exposed credential.
