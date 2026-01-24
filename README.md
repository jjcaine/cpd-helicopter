# ADS-B Exchange Flight Data Fetcher

Extract individual flight legs from ADS-B Exchange and export to CSV format.

## Features

- Fetches historic flight trace data from ADS-B Exchange
- Automatically detects individual flight legs (separates flights by gaps in tracking data)
- Filters flights by date range
- Outputs CSV format with flight start/end times
- All times are in UTC

## Installation

```bash
npm install
npx playwright install chromium
```

## Usage

### Generate CSV of Flight Legs

```bash
node flights-to-csv.js <ICAO> <START_DATE> [END_DATE]
```

**Arguments:**
- `ICAO` - Aircraft ICAO hex code (e.g., `ad3c55`)
- `START_DATE` - Start date in `YYYY-MM-DD` format
- `END_DATE` - Optional end date (defaults to same as start date)

**Examples:**

```bash
# Get flights for a single day
node flights-to-csv.js ad3c55 2025-12-16

# Get flights for a date range
node flights-to-csv.js ad3c55 2025-08-15 2025-09-06
```

**Output format:**
```csv
Date,Start Time (UTC),End Time (UTC)
2025-12-16,18:43:54,20:05:07
2025-12-16,21:30:39,22:59:20
2025-12-17,0:12:01,2:11:03
```

**Redirecting to file:**
```bash
node flights-to-csv.js ad3c55 2025-12-16 2025-12-17 > flights.csv
```

## How It Works

1. **Fetch Data**: For each date in the range, uses Playwright to load the ADS-B Exchange page with the `showTrace` parameter to access historical data
2. **Parse Legs**: Analyzes the trace data for gaps (default: 5+ minutes) to identify separate flights
3. **Deduplicate**: Removes any duplicate entries and sorts by start time
4. **Output**: Formats as CSV with Date, Start Time, and End Time

### Historical Data Access

The script uses ADS-B Exchange's `showTrace` parameter to access historical flight data. For each date in your range, it loads:
```
https://globe.adsbexchange.com/?icao=<ICAO>&showTrace=<DATE>
```

This allows retrieval of historical data beyond just the most recent 24-48 hours.

### Flight Leg Detection

The script identifies individual flight legs by looking for gaps in the tracking data. By default, any gap of 5 minutes or more between data points is considered a new flight leg. This effectively separates landings and takeoffs.

## Time Zone Note

All times in the CSV output are in **UTC**. The date column shows the UTC date when the flight started.

For example, a flight in Cleveland (EST/UTC-5) that takes off at 7:12 PM local time on Dec 16 will show as:
```
2025-12-17,0:12:01,2:11:03
```
Because 7:12 PM EST on Dec 16 = 00:12 UTC on Dec 17.

## Finding ICAO Codes

1. Visit [globe.adsbexchange.com](https://globe.adsbexchange.com/)
2. Search for the aircraft by registration number
3. The ICAO code is in the URL after `?icao=`

Example: For registration N952CP, the URL `https://globe.adsbexchange.com/?icao=ad3c55` shows ICAO is `ad3c55`

## Data Availability

- ADS-B Exchange retains historical flight trace data
- Access historical data using the `showTrace` parameter (automatically handled by the script)
- Some dates may have no data (no flights, or data not retained)
- The script gracefully handles dates with no available data

## Programmatic Usage

```javascript
const { fetchTraceData, parseFlightLegs, filterLegsByDateRange, legsToCSV } = require('./flights-to-csv');

async function example() {
  // Fetch raw trace data
  const traceData = await fetchTraceData('ad3c55');

  // Parse into individual legs
  const allLegs = parseFlightLegs(traceData);

  // Filter by date
  const filteredLegs = filterLegsByDateRange(allLegs, '2025-12-16', '2025-12-17');

  // Convert to CSV
  const csv = legsToCSV(filteredLegs);

  console.log(csv);
}
```

## Adjusting Gap Threshold

The default gap threshold is 5 minutes (300 seconds). You can adjust this in the code:

```javascript
// Use 10 minute gaps instead
const legs = parseFlightLegs(traceData, 600);
```
