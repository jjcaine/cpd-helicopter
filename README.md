# CPD Helicopter Flight Tracker

Track CPD helicopter flights using ADS-B Exchange data, storing results in a PostgreSQL database with automatic daily syncing via GitHub Actions.

## Features

- Fetches historical flight trace data from ADS-B Exchange
- Captures detailed flight telemetry (position, altitude, speed, heading, etc.)
- Stores flight data in PostgreSQL with automatic deduplication
- UPSERT logic handles in-progress flights gracefully
- Supports multiple tracked aircraft
- Daily automated sync via GitHub Actions
- CSV backfill for importing historical data
- Telemetry backfill for flights synced before telemetry capture was added

## Tracked Aircraft

| ICAO | Registration | Description |
|------|--------------|-------------|
| ad389e | - | CPD Helicopter 1 |
| ad3c55 | N952CP | CPD Helicopter 2 |

## Installation

### Prerequisites

- Python 3.11+
- PostgreSQL database
- [uv](https://github.com/astral-sh/uv) package manager (recommended)

### Setup

```bash
# Create virtual environment
uv venv

# Install dependencies
uv pip install -r requirements.txt

# Install Playwright browser
source .venv/bin/activate
playwright install chromium

# Configure database
cp .env.example .env
# Edit .env with your database credentials
```

## Usage

### Fetch Yesterday's Flights (Daily Sync)

```bash
python -m src.main --yesterday
```

This fetches flights for all tracked aircraft from yesterday and upserts them into the database.

### Fetch Specific Date Range

```bash
# Single aircraft, single date
python -m src.main --icao ad389e --start-date 2025-12-16

# Single aircraft, date range
python -m src.main --icao ad389e --start-date 2025-12-01 --end-date 2025-12-31

# All tracked aircraft, date range
python -m src.main --start-date 2025-12-16 --end-date 2025-12-17
```

### Skip Telemetry Capture

By default, syncing captures detailed telemetry (GPS coordinates, altitude, speed, etc.) for each flight. To store only flight start/end times:

```bash
python -m src.main --yesterday --no-telemetry
```

### Backfill Telemetry

Retroactively fetch and store telemetry for existing flights that don't have it (e.g., flights synced before telemetry capture was added, or synced with `--no-telemetry`):

```bash
# Backfill all flights missing telemetry
python -m src.main --backfill-telemetry

# Limit to a specific aircraft
python -m src.main --backfill-telemetry --icao ad389e

# Limit to a date range
python -m src.main --backfill-telemetry --start-date 2025-10-01 --end-date 2025-10-31
```

Flights are grouped by aircraft and date to minimize ADS-B Exchange fetches. Fetched legs are matched to existing flights by start time (within a 60-second tolerance).

### Backfill from CSV

Import historical data from CSV files:

```bash
# Auto-detect ICAO from filename
python -m src.main --backfill ad389e_flights.csv

# Specify ICAO explicitly
python -m src.main --backfill flights.csv --icao ad3c55
```

**Expected CSV format:**
```csv
Date,Start Time (UTC),End Time (UTC)
2025-12-16,18:43:54,20:05:07
2025-12-16,21:30:39,22:59:20
```

## CLI Reference

| Flag | Description |
|------|-------------|
| `--yesterday` | Fetch flights for yesterday (UTC) |
| `--start-date YYYY-MM-DD` | Start date for fetching flights |
| `--end-date YYYY-MM-DD` | End date (defaults to start date if not specified) |
| `--icao XXXXXX` | Limit to a specific aircraft ICAO code |
| `--no-telemetry` | Skip telemetry capture, only store flight start/end times |
| `--backfill CSV_FILE` | Import historical flights from a CSV file |
| `--backfill-telemetry` | Retroactively fetch telemetry for flights that have none |

## Database Schema

### Flights Table

| Column | Type | Description |
|--------|------|-------------|
| id | SERIAL | Primary key |
| icao | VARCHAR(6) | Aircraft ICAO hex code |
| start_time | TIMESTAMPTZ | Flight start time (UTC) |
| end_time | TIMESTAMPTZ | Flight end time (UTC) |
| created_at | TIMESTAMPTZ | Record creation time |
| updated_at | TIMESTAMPTZ | Last update time |

**Unique Constraint:** `(icao, start_time)` - enables UPSERT logic

### Flight Telemetry Table

| Column | Type | Description |
|--------|------|-------------|
| id | SERIAL | Primary key |
| flight_id | INTEGER | Foreign key to flights (CASCADE delete) |
| timestamp | TIMESTAMPTZ | Telemetry point time (UTC) |
| latitude | FLOAT | Latitude |
| longitude | FLOAT | Longitude |
| altitude | INTEGER | Barometric altitude (feet), null if on ground |
| altitude_ground | BOOLEAN | Whether aircraft is on the ground |
| ground_speed | FLOAT | Ground speed (knots) |
| track | FLOAT | Track/heading (degrees) |
| vertical_rate | INTEGER | Vertical rate (fpm) |
| geo_altitude | INTEGER | Geometric altitude (feet) |
| ias | INTEGER | Indicated airspeed (knots) |
| roll_angle | FLOAT | Roll angle (degrees) |

### UPSERT Strategy

The system uses PostgreSQL's UPSERT to handle in-progress flights:

1. **First sync:** Flight in progress → Insert with provisional `end_time`
2. **Later sync:** Same flight completed → Update `end_time` to final value
3. **Subsequent syncs:** Same flight, same times → No change (idempotent)

This allows multiple daily runs without creating duplicate records, and automatically updates flights that were in-progress during earlier syncs.

### Example Queries

```sql
-- Flights per day
SELECT DATE(start_time AT TIME ZONE 'UTC') as date, COUNT(*)
FROM flights
GROUP BY date
ORDER BY date DESC;

-- Flight duration statistics
SELECT icao,
       COUNT(*) as flights,
       AVG(end_time - start_time) as avg_duration,
       SUM(end_time - start_time) as total_time
FROM flights
GROUP BY icao;

-- Recent flights
SELECT icao, start_time, end_time,
       end_time - start_time as duration
FROM flights
ORDER BY start_time DESC
LIMIT 10;

-- Flights with telemetry point counts
SELECT f.icao, f.start_time, f.end_time,
       COUNT(t.id) as telemetry_points
FROM flights f
LEFT JOIN flight_telemetry t ON f.id = t.flight_id
GROUP BY f.id
ORDER BY f.start_time DESC
LIMIT 10;
```

## GitHub Actions

The repository includes a GitHub Actions workflow that runs daily at 6 AM UTC to sync yesterday's flights.

### Required Secrets

Configure these in your repository settings (Settings → Secrets → Actions):

- `DB_USER` - Database username
- `DB_PASSWORD` - Database password
- `DB_HOST` - Database host
- `DB_PORT` - Database port
- `DB_NAME` - Database name

### Manual Trigger

You can manually trigger the workflow from the Actions tab using "Run workflow".

## Development

### Running Tests

```bash
source .venv/bin/activate
python -m pytest tests/ -v
```

### Project Structure

```
cpd-helicopter/
├── .github/workflows/
│   └── daily-sync.yml      # GitHub Actions workflow
├── src/
│   ├── __init__.py
│   ├── database.py         # Database connection & UPSERT logic
│   ├── main.py             # CLI entry point
│   ├── models.py           # SQLAlchemy models
│   └── scraper.py          # ADS-B Exchange scraping
├── tests/
│   ├── test_database.py    # Database tests
│   ├── test_main.py        # CLI tests
│   ├── test_scraper.py     # Scraper tests
│   └── test_telemetry.py   # Telemetry storage tests
├── config.py               # Configuration
├── requirements.txt        # Dependencies
└── .env.example            # Environment template
```

## How It Works

1. **Fetch Data:** Uses Playwright to load ADS-B Exchange with the `showTrace` parameter for historical data
2. **Parse Legs:** Analyzes trace data for 5+ minute gaps to identify separate flight legs
3. **Capture Telemetry:** Extracts GPS coordinates, altitude, speed, heading, and other data points from each trace
4. **Deduplicate:** Removes duplicates based on start time
5. **UPSERT:** Inserts new flights or updates end times for existing flights, storing associated telemetry

## Time Zone Note

All times are stored and displayed in **UTC**. A flight in Cleveland (EST/UTC-5) that takes off at 7:12 PM local on Dec 16 will be stored as:
- `start_time`: 2025-12-17 00:12:00+00
- Because 7:12 PM EST = 00:12 UTC (next day)

## Data Availability

- ADS-B Exchange retains historical data for a limited time
- Recent data (last few days) is most reliable
- Some dates may have no data if the aircraft didn't fly
- The script gracefully handles dates with no available data

## Finding ICAO Codes

1. Visit [globe.adsbexchange.com](https://globe.adsbexchange.com/)
2. Search for the aircraft by registration number
3. The ICAO code is in the URL after `?icao=`

Example: `https://globe.adsbexchange.com/?icao=ad3c55` → ICAO is `ad3c55`
