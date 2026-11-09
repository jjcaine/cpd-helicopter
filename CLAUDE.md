# CLAUDE.md

## Project Overview

Cleveland Police Department helicopter flight tracker. Collects public transponder data from CPD helicopters and stores it in a PostgreSQL database with detailed telemetry (GPS, altitude, speed, heading, etc.). Automated daily via GitHub Actions.

## Tracked Aircraft

| ICAO | Registration | Notes |
|------|-------------|-------|
| ad389e | N951CP | CPD helicopter |
| ad3c55 | N952CP | CPD helicopter |

CPD rotates between these helicopters — they don't fly simultaneously. Extended gaps in one aircraft's data are normal, not a bug.

## Tech Stack

- **Language:** Python 3.11+
- **Package manager:** uv
- **Trace data:** adsb.lol daily `globe_history` GitHub releases (ODbL), streamed through `tar` (`src/trace_source.py`)
- **Database:** PostgreSQL on DigitalOcean (managed, SSL required)
- **ORM:** SQLAlchemy
- **Data export:** Parquet files via PyArrow (committed to repo for public access)
- **Notebooks:** marimo (auto-exported to ipynb)
- **CI/CD:** GitHub Actions

## Key Commands

```bash
# Install and setup
uv sync

# Run tests (needs PostgreSQL — use docker-compose up -d for local)
uv run pytest tests/ -v

# Fetch yesterday's flights
uv run python -m src.main --yesterday

# Fetch specific dates
uv run python -m src.main --start-date 2026-03-01 --end-date 2026-03-15

# Auto-backfill missed days (used by daily sync)
uv run python -m src.main --auto-backfill

# Export database to Parquet
PYTHONPATH=. uv run python scripts/export_data.py
```

## Architecture

```
adsb.lol globe_history archives → trace fetcher → PostgreSQL (DigitalOcean) → Parquet export → Git repo
```

- **Source of truth:** PostgreSQL database on DigitalOcean
- **Public access:** Parquet files in `data/` committed daily by GitHub Actions bot
- **Analysis:** marimo notebook in `notebooks/` (reads from DB or Parquet)

The daily sync workflow pushes "Update data export" commits, so local repo drifts behind remote. The database always has the most current data.

## Database

Two tables: `flights` (metadata; upsert matches overlapping time ranges per aircraft) and `flight_telemetry` (detailed position/altitude/speed points, FK to flights).

Connection configured via `.env` file (see `.env.example`). Production uses DigitalOcean managed PostgreSQL with SSL. Tests use a local PostgreSQL instance (docker-compose.yml provided).

## GitHub Actions Workflows

- **daily-sync.yml** — Runs 6 AM UTC daily. Uses `--auto-backfill` to fetch missing days, exports to Parquet, commits and pushes.
- **backfill-telemetry.yml** — Manual dispatch. Retroactively fetches telemetry for flights that don't have it.
- **export-notebooks.yml** — Triggered on changes to `notebooks/*.py`. Converts marimo to ipynb.
- **test.yml** — Runs on PRs/pushes to main. Spins up PostgreSQL service container.
- **dedupe-flights.yml** — Manual dispatch. Merges overlapping duplicate flights (dry run unless `apply` is checked).

## How Fetching Works

1. For each UTC day, `src/trace_source.py` streams the `v<YYYY.MM.DD>-planes-readsb-prod-0` release from `adsblol/globe_history_<YYYY>` (one download per day, shared by all aircraft, cached in-process)
2. `tar` extracts only `traces/<last2>/trace_full_<icao>.json` (gzip JSON, readsb trace format)
3. Parses trace points into flight legs (5+ minute gap = new leg)
4. Each leg gets full telemetry extracted
5. Flights are upserted: an existing flight for the same aircraft with an overlapping [start, end] range is the same flight (sources differ by up to ~1 min on start). Auto-backfill starts after the fleet's newest flight and always re-checks the last 3 days.

Source failures raise `TraceSourceError` and fail the job; "no trace" for a day is normal. ADS-B Exchange is no longer usable: its historical traces are behind a Cloudflare Turnstile human check (since ~2026-10-27), and we don't automate around that.

All times are stored in UTC.
