# Distributor Catalog Integration

A Python data-integration service that brings supplier catalogs into a shared product model for search, price comparison, and spreadsheet export. The repository combines distributor-specific connectors, an asynchronous database layer, a search API, and a lightweight browser interface.

[Architecture](#architecture) · [Setup requirements](#setup-requirements) · [Review status](#review-status)

## What to review

- **Integration boundaries:** each distributor has its own parsing and export module, accommodating different API formats and update flows.
- **Data normalization:** product cleanup and upsert logic are shared instead of being repeated in every connector.
- **Operator-facing results:** search endpoints expose grouped products, name search, and CSV/Excel exports; update notifications report ingestion progress.

## Architecture

```text
Supplier APIs → distributor connectors → normalization / upsert → SQL database
                                                                   ↓
                                      search API → browser UI / CSV / Excel
```

| Source | Responsibility |
|---|---|
| [main.py](main.py) | Update scheduling, distributor orchestration, and integrity checks |
| [core/utils.py](core/utils.py), [core/upsert.py](core/upsert.py) | Product cleanup and database writes |
| [core/db_models.py](core/db_models.py), [core/db_engine.py](core/db_engine.py) | SQLAlchemy product model and async engine |
| [api.py](api.py) | FastAPI search, grouping, currency-rate, and export endpoints |
| [server.py](server.py), [static/index.html](static/index.html) | Flask/browser presentation layer |
| [core/telegram_notify.py](core/telegram_notify.py) | Export progress and operational notifications |
| `marvel/`, `merlion/`, `netlab/`, `ocs/`, `treolan/`, `vvp/`, `resursmedio/` | Supplier-specific integration code |

**Stack:** Python, asyncio, SQLAlchemy, Requests/HTTPX, SOAP via Zeep, pandas, FastAPI, Flask, and HTML. The source contains MySQL configuration support and a PostgreSQL fallback; deployment must choose and provision the matching database driver.

## Setup requirements

Start from an isolated checkout and virtual environment:

```bash
git clone https://github.com/arar228/parser_dist.git
cd parser_dist
python -m venv .venv
# Activate .venv using your shell's activation command.
python -m pip install -r requirements.txt
```

The dependency file is the starting point, not a verified complete environment. Source imports also include `fastapi`, `flask`, `httpx`, `defusedxml`, `tqdm`, and the PostgreSQL `asyncpg` dialect; reconcile the required subset with the deployment before starting services.

Configuration currently spans environment variables, external configuration, and connector constants:

| Source | Names to provision or review |
|---|---|
| Existing environment readers | `MERLION_CLIENT_ID`, `MERLION_LOGIN`, `MERLION_PASSWORD`, `VVP_LOGIN`, `VVP_PASSWORD` |
| External database module | `mysql_config.MYSQL_URL` |
| Database fallback configuration | `DB_USER`, `DB_PASS`, `DB_HOST`, `DB_PORT`, `DB_NAME` |
| Notification configuration | `TELEGRAM_BOT_TOKEN`, `TELEGRAM_CHAT_ID` |
| Connector credentials | Supplier-specific login/password/API-key settings in the corresponding modules |

The latter rows are configuration names, **not a claim that the current branch reads each one from the environment**. Credential-source migration is a separate security change. Use the [Russian MySQL migration guide](MYSQL_MIGRATION_GUIDE.md) as historical deployment context and reconcile it with the actual running configuration.

`main.py` is the ingestion entry point. `api.py` and `server.py` provide separate serving surfaces. Confirm database ownership, schema, supplier access, and notification recipients before running them. `core/db_create_tables.py` includes a destructive table-cleanup path and must be treated as an administrative operation.

## Review status

Documentation was reviewed against the source tree on **2026-09-07**. This pass did not run supplier calls, database writes, Telegram notifications, or an end-to-end deployment. No GitHub Actions workflow is included in this snapshot.

Current source/history requires coordinated credential hygiene; repository visibility and `.gitignore` do not establish that credentials are safe. Keep actual keys, database exports, and production logs out of portfolio examples. Useful next verification is a fixture-backed connector test followed by a disposable database ingestion/search check.
