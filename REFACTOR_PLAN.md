# Python Refactor Plan: CPD Helicopter Flight Tracker

## Overview
Refactor the Node.js flight tracking project to Python with PostgreSQL database storage and daily GitHub Actions scheduling.

## Current State
- Node.js application using Playwright to scrape ADS-B Exchange
- Outputs to CSV with columns: Date, Start Time (UTC), End Time (UTC)
- Parses flight legs from trace data based on 5-minute gaps
- Supports date range queries

## Target State
- Python application using Playwright Python
- PostgreSQL database storage via SQLAlchemy ORM
- Two timestamp columns: `start_time`, `end_time` (not separate date/time fields)
- UPSERT strategy to handle in-progress flights
- Daily automated runs via GitHub Actions
- Secure credential management

---

## Phase 1: Python Project Setup

### 1.1 Project Structure
```
cpd-helicopter/
├── .github/
│   └── workflows/
│       └── daily-sync.yml          # GitHub Actions workflow
├── src/
│   ├── __init__.py
│   ├── scraper.py                  # ADS-B Exchange scraping logic
│   ├── models.py                   # SQLAlchemy models
│   ├── database.py                 # Database connection/session management
│   └── main.py                     # Main entry point
├── tests/
│   ├── __init__.py
│   └── test_scraper.py
├── .env.example                     # Example environment variables
├── .gitignore                       # Python-specific gitignore
├── requirements.txt                 # Python dependencies
├── README.md                        # Updated documentation
└── config.py                        # Configuration management
```

### 1.2 Dependencies
```
playwright>=1.40.0                   # Browser automation
sqlalchemy>=2.0.0                    # ORM
psycopg2-binary>=2.9.9              # PostgreSQL driver
python-dotenv>=1.0.0                # Environment variable management
```

---

## Phase 2: Database Design

### 2.1 Database Schema

**Table: `flights`**
| Column | Type | Constraints | Description |
|--------|------|-------------|-------------|
| `id` | SERIAL | PRIMARY KEY | Auto-incrementing ID |
| `icao` | VARCHAR(6) | NOT NULL, INDEX | Aircraft ICAO hex code |
| `start_time` | TIMESTAMPTZ | NOT NULL | Flight start time (UTC) |
| `end_time` | TIMESTAMPTZ | NOT NULL | Flight end time (UTC) |
| `created_at` | TIMESTAMPTZ | DEFAULT NOW() | Record creation timestamp |
| `updated_at` | TIMESTAMPTZ | DEFAULT NOW() | Record update timestamp |

**Indexes:**
- Primary key on `id`
- Index on `icao` for fast aircraft lookups
- Index on `start_time` for time-based queries
- **Unique constraint on `(icao, start_time)`** to enable UPSERT logic

**Derived Data (via queries):**
- Flight date: `DATE(start_time AT TIME ZONE 'UTC')`
- Flight duration: `end_time - start_time`
- Daily flight count: `COUNT(*) GROUP BY DATE(start_time)`

### 2.2 SQLAlchemy Model
```python
from sqlalchemy import Column, Integer, String, DateTime, Index, UniqueConstraint
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.sql import func

Base = declarative_base()

class Flight(Base):
    __tablename__ = 'flights'

    id = Column(Integer, primary_key=True)
    icao = Column(String(6), nullable=False, index=True)
    start_time = Column(DateTime(timezone=True), nullable=False, index=True)
    end_time = Column(DateTime(timezone=True), nullable=False)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())

    __table_args__ = (
        UniqueConstraint('icao', 'start_time', name='uix_icao_start_time'),
        Index('ix_start_time', 'start_time'),
    )
```

### 2.3 In-Progress Flight Handling (UPSERT Strategy)

**The Problem:**
When the daily sync runs, some flights may still be in-progress (aircraft in the air). The trace data will show the last known position as the end_time, but this is provisional.

**The Solution:**
Use PostgreSQL's UPSERT capability:
```sql
INSERT INTO flights (icao, start_time, end_time, created_at, updated_at)
VALUES (%s, %s, %s, NOW(), NOW())
ON CONFLICT (icao, start_time)
DO UPDATE SET
    end_time = EXCLUDED.end_time,
    updated_at = NOW()
WHERE EXCLUDED.end_time > flights.end_time;
```

**How It Works:**
1. First run: Flight starts at 11 PM, still in air at 6 AM sync → Insert with provisional end_time
2. Second run (next day): Same flight now shows complete end_time → UPDATE end_time to final value
3. Subsequent runs: Same flight with same end_time → No update (WHERE clause filters)

**Implementation in SQLAlchemy:**
```python
from sqlalchemy.dialects.postgresql import insert

stmt = insert(Flight).values(
    icao=flight.icao,
    start_time=flight.start_time,
    end_time=flight.end_time
)

stmt = stmt.on_conflict_do_update(
    index_elements=['icao', 'start_time'],
    constraint='uix_icao_start_time',
    set_={
        'end_time': stmt.excluded.end_time,
        'updated_at': func.now()
    },
    where=(stmt.excluded.end_time > Flight.end_time)
)

session.execute(stmt)
```

**Benefits:**
- Automatic handling of in-progress flights
- No manual status tracking needed
- Data converges to correct state over multiple runs
- Simple and robust

---

## Phase 3: Scraper Migration to Python

### 3.1 Core Functions to Port
1. `fetchTraceData()` - Fetch trace data using Playwright Python
2. `parseFlightLegs()` - Parse trace data into flight legs
3. `filterLegsByDateRange()` - Filter legs by date range
4. Date range generation

### 3.2 Implementation Approach
- Use `playwright-python` for browser automation (matches current stack)
- Intercept network responses to capture trace JSON
- Parse flight legs with same 5-minute gap logic
- Return Flight model instances instead of CSV

### 3.3 Key Differences from Node.js
- Async/await syntax: use `asyncio` and `async/await`
- Datetime handling: use `datetime` module with UTC timezone
- JSON parsing: use built-in `json` module

---

## Phase 4: Database Integration

### 4.1 Connection Configuration
```python
# config.py
import os
from dotenv import load_dotenv

load_dotenv()

DATABASE_CONFIG = {
    'user': os.getenv('DB_USER', 'doadmin'),
    'password': os.getenv('DB_PASSWORD'),
    'host': os.getenv('DB_HOST', 'db-postgresql-nyc3-13586-do-user-12959197-0.j.db.ondigitalocean.com'),
    'port': os.getenv('DB_PORT', '25060'),
    'database': os.getenv('DB_NAME', 'defaultdb'),
    'sslmode': 'require'
}

def get_database_url():
    return (
        f"postgresql://{DATABASE_CONFIG['user']}:{DATABASE_CONFIG['password']}"
        f"@{DATABASE_CONFIG['host']}:{DATABASE_CONFIG['port']}/{DATABASE_CONFIG['database']}"
        f"?sslmode={DATABASE_CONFIG['sslmode']}"
    )
```

### 4.2 Session Management
```python
# database.py
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from config import get_database_url

engine = create_engine(get_database_url())
SessionLocal = sessionmaker(bind=engine)

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
```

### 4.3 Data Insertion Strategy
- Use UPSERT (INSERT ... ON CONFLICT) for all flights
- Update end_time if we see a later value for same start_time
- Batch insert for efficiency when processing multiple flights
- Log inserted vs updated flights for debugging

---

## Phase 5: Main Script

### 5.1 CLI Interface
```bash
# Fetch and store flights for a single aircraft
python -m src.main --icao ad389e --start-date 2025-12-16 --end-date 2025-12-17

# Fetch for multiple aircraft (daily sync default)
python -m src.main --yesterday

# Fetch specific aircraft for yesterday
python -m src.main --icao ad389e --yesterday

# Backfill from CSV files
python -m src.main --backfill flights.csv
python -m src.main --backfill ad389e_flights.csv
```

### 5.2 Aircraft Configuration
Store tracked aircraft in config file:
```python
# config.py
TRACKED_AIRCRAFT = [
    'ad389e',
    'ad3c55'
]
```

### 5.3 Main Logic Flow

**Normal Sync Mode:**
1. Parse command-line arguments
2. Determine aircraft list (from config or CLI)
3. Generate date range
4. For each aircraft:
   - For each date:
     - Fetch trace data from ADS-B Exchange
     - Parse flight legs
     - Create Flight model instances
     - Log "No flights found for [icao] on [date]" if empty
5. Open database session
6. UPSERT all flights (insert new, update end_time if changed)
7. Commit transaction
8. Log results (new flights, updated flights, unchanged)

**Backfill Mode:**
1. Parse CSV file (Date, Start Time UTC, End Time UTC)
2. Extract ICAO from filename (e.g., `ad389e_flights.csv` → `ad389e`)
3. Convert to Flight model instances
4. UPSERT to database
5. Log results

---

## Phase 5B: CSV Backfill

### 5B.1 Backfill Strategy
Import existing CSV data into the database using UPSERT logic.

**CSV Format (existing files):**
```csv
Date,Start Time (UTC),End Time (UTC)
2025-12-16,18:43:54,20:05:07
2025-12-16,21:30:39,22:59:20
```

**Implementation:**
1. Read CSV file
2. Extract ICAO from filename (e.g., `ad389e_flights.csv` → `ad389e`)
3. Parse each row:
   - Combine Date + Start Time → `start_time` (as timezone-aware datetime)
   - Combine Date + End Time → `end_time` (as timezone-aware datetime)
   - Handle midnight rollover (if end time < start time, increment date)
4. Create Flight instances
5. UPSERT to database
6. Log results (inserted, skipped duplicates)

**CLI Usage:**
```bash
# Backfill from specific CSV
python -m src.main --backfill flights.csv --icao ad389e

# Backfill from file with ICAO in name
python -m src.main --backfill ad389e_flights.csv  # Auto-detects ICAO
```

---

## Phase 6: GitHub Actions Setup

### 6.1 Workflow Configuration
```yaml
name: Daily Flight Sync
on:
  schedule:
    - cron: '0 6 * * *'  # Run at 6 AM UTC daily
  workflow_dispatch:  # Allow manual triggering

jobs:
  sync:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4

      - name: Set up Python
        uses: actions/setup-python@v5
        with:
          python-version: '3.11'

      - name: Install dependencies
        run: |
          pip install -r requirements.txt
          playwright install chromium

      - name: Fetch yesterday's flights for all aircraft
        env:
          DB_USER: ${{ secrets.DB_USER }}
          DB_PASSWORD: ${{ secrets.DB_PASSWORD }}
          DB_HOST: ${{ secrets.DB_HOST }}
          DB_PORT: ${{ secrets.DB_PORT }}
          DB_NAME: ${{ secrets.DB_NAME }}
        run: |
          python -m src.main --yesterday

      - name: Notify on success
        if: success()
        run: |
          echo "✅ Daily sync completed successfully"
          # GitHub Actions automatically sends email notifications for workflow runs
          # Can add Slack/Discord webhook here if needed

      - name: Notify on failure
        if: failure()
        run: |
          echo "❌ Daily sync failed"
          # GitHub Actions automatically sends email notifications for failures
```

### 6.2 GitHub Secrets Required
- `DB_USER`: doadmin
- `DB_PASSWORD`: (configured in GitHub secrets)
- `DB_HOST`: (configured in GitHub secrets)
- `DB_PORT`: 25060
- `DB_NAME`: defaultdb

### 6.3 Schedule Strategy
- Run daily at 6 AM UTC (after most overnight flights complete)
- Fetch previous day's flights (yesterday) for both aircraft (`ad389e` and `ad3c55`)
- UPSERT handles in-progress flights automatically
- In-progress flights from overnight get updated on subsequent runs

### 6.4 Notifications
GitHub Actions provides built-in notifications:
- **Email**: Automatic on workflow failure (and optionally on success)
- **Configure in**: GitHub Settings → Notifications → Actions
- **Enable**: "Send notifications for failed workflows"
- **For success alerts**: Can enable email notifications for all workflow runs (at least initially to confirm daily runs)
- **Optional**: Can add Slack/Discord webhooks for custom notifications

---

## Phase 7: Testing & Validation

### 7.1 Unit Tests
- Test flight leg parsing logic
- Test date range generation
- Test UPSERT logic (mock database)

### 7.2 Integration Tests
- Test database connection
- Test flight insertion
- Test in-progress flight updates
- Test duplicate handling

### 7.3 Manual Validation
1. Run script for known date with flights
2. Verify flights appear in database
3. Check timestamps are correct (UTC)
4. Simulate in-progress flight (insert, then update)
5. Test GitHub Action manually via workflow_dispatch

---

## Phase 8: Documentation Updates

### 8.1 README Updates
- Update installation instructions for Python
- Document database setup steps
- Explain UPSERT strategy for in-progress flights
- Explain GitHub Actions configuration
- Add example queries for derived data

### 8.2 Migration Guide
- Document steps to migrate from CSV to database
- Provide SQL queries for common analytics

---

## Implementation Order

1. **Setup Phase** (Create Python project structure, dependencies)
2. **Database Phase** (Create models, connection, test connection)
3. **Scraper Phase** (Port scraping logic to Python)
4. **Integration Phase** (Connect scraper to database with UPSERT logic)
5. **CLI Phase** (Build command-line interface)
6. **Testing Phase** (Test UPSERT edge cases, validate end-to-end)
7. **Automation Phase** (GitHub Actions workflow)
8. **Documentation Phase** (Update docs, clean up old files)

---

## Migration Checklist

- [ ] Create Python project structure
- [ ] Install dependencies
- [ ] Create SQLAlchemy models with UPSERT constraint
- [ ] Set up database connection
- [ ] Create database table
- [ ] Port scraping logic to Python
- [ ] Add multi-aircraft support (ad389e, ad3c55)
- [ ] Implement UPSERT database insertion
- [ ] Build CLI interface
- [ ] Implement CSV backfill functionality
- [ ] Test UPSERT with in-progress flights
- [ ] Test local execution for both aircraft
- [ ] Backfill historical data from CSV files
- [ ] Create .env.example file
- [ ] Set up GitHub Actions workflow
- [ ] Configure GitHub secrets
- [ ] Configure GitHub Actions notifications
- [ ] Test GitHub Actions manually
- [ ] Update README with UPSERT explanation and multi-aircraft support
- [ ] Archive old Node.js files
- [ ] Clean up repository

---

## Decisions & Requirements

1. **Aircraft**: Track two ICAOs daily: `ad389e` and `ad3c55` (support for more in future)
2. **Backfill**: Yes, import historical data from existing CSV files
3. **Error Handling**: Days with no flights should explicitly log "No flights found for [date]" and continue
4. **Notifications**: GitHub Actions should send both failure AND success alerts (at least initially)
5. **Retention**: Keep all data forever (no purging)

---

## Estimated File Changes

**New Files:**
- `src/__init__.py`
- `src/scraper.py` (~150 lines)
- `src/models.py` (~40 lines)
- `src/database.py` (~30 lines)
- `src/main.py` (~100 lines)
- `config.py` (~30 lines)
- `requirements.txt` (~5 lines)
- `.github/workflows/daily-sync.yml` (~30 lines)
- `.env.example` (~10 lines)
- `tests/__init__.py`
- `tests/test_scraper.py` (~50 lines)

**Modified Files:**
- `README.md` (complete rewrite for Python)
- `.gitignore` (add Python-specific ignores: `__pycache__/`, `*.pyc`, `.env`, `venv/`)

**Archived/Removed Files:**
- `flights-to-csv.js` (can remove or keep for reference)
- `package.json` (can remove)
- `package-lock.json` (can remove)
- `*.csv` files (can archive or use for backfill testing)

---

## Risk Assessment

**Low Risk:**
- Playwright Python is stable and well-documented
- SQLAlchemy is mature and reliable
- PostgreSQL connection is straightforward
- UPSERT is a native PostgreSQL feature (well-supported)

**Medium Risk:**
- ADS-B Exchange might change their trace data format
- Network requests might occasionally fail (need retry logic)

**Mitigation:**
- Add robust error handling
- Log all scraping attempts
- Use GitHub Actions workflow notifications for failures
- Test UPSERT logic thoroughly with edge cases
