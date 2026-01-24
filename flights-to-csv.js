#!/usr/bin/env node
const { chromium } = require('playwright');

/**
 * Fetches trace data from ADS-B Exchange for a specific date
 * @param {string} icao - Aircraft ICAO code
 * @param {string} date - Date in YYYY-MM-DD format (optional)
 */
async function fetchTraceData(icao, date = null) {
  const browser = await chromium.launch({ headless: true });
  const context = await browser.newContext({
    userAgent: 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
  });

  const page = await context.newPage();
  let traceData = null;

  page.on('response', async (response) => {
    const url = response.url();
    if ((url.includes('trace_full') || url.includes('trace_recent')) && url.endsWith('.json')) {
      try {
        const data = await response.json();
        if (!traceData || (url.includes('trace_full'))) {
          traceData = data;
        }
      } catch (e) {}
    }
  });

  // Build URL with optional showTrace parameter
  let url = `https://globe.adsbexchange.com/?icao=${icao}`;
  if (date) {
    url += `&showTrace=${date}`;
  }

  try {
    await page.goto(url, {
      waitUntil: 'load',
      timeout: 15000
    });
  } catch (e) {}

  await page.waitForTimeout(5000);
  await browser.close();

  if (!traceData || !traceData.trace) {
    throw new Error('No trace data found');
  }

  return traceData;
}

/**
 * Parses trace data into individual flight legs
 * @param {Object} traceData - Raw trace data from ADS-B Exchange
 * @param {number} gapThreshold - Minimum gap in seconds to consider a new leg (default 300 = 5 min)
 * @returns {Array} Array of flight legs with start/end times
 */
function parseFlightLegs(traceData, gapThreshold = 300) {
  const baseTimestamp = traceData.timestamp;
  const trace = traceData.trace;

  if (!trace || trace.length === 0) {
    return [];
  }

  const legs = [];
  let currentLeg = {
    startIndex: 0,
    endIndex: 0
  };

  for (let i = 1; i < trace.length; i++) {
    const timeDiff = trace[i][0] - trace[i-1][0];

    // If we find a gap, save the current leg and start a new one
    if (timeDiff > gapThreshold) {
      currentLeg.endIndex = i - 1;
      legs.push({
        startTime: new Date((baseTimestamp + trace[currentLeg.startIndex][0]) * 1000),
        endTime: new Date((baseTimestamp + trace[currentLeg.endIndex][0]) * 1000)
      });

      currentLeg = {
        startIndex: i,
        endIndex: i
      };
    } else {
      currentLeg.endIndex = i;
    }
  }

  // Don't forget the last leg
  if (currentLeg.startIndex < trace.length) {
    legs.push({
      startTime: new Date((baseTimestamp + trace[currentLeg.startIndex][0]) * 1000),
      endTime: new Date((baseTimestamp + trace[currentLeg.endIndex][0]) * 1000)
    });
  }

  return legs;
}

/**
 * Filters flight legs by date range
 */
function filterLegsByDateRange(legs, startDate, endDate = null) {
  // Parse dates as UTC to match the leg timestamps
  const start = new Date(startDate + 'T00:00:00Z');

  let end;
  if (endDate) {
    end = new Date(endDate + 'T23:59:59.999Z');
  } else {
    // If no end date, use same day as start
    end = new Date(startDate + 'T23:59:59.999Z');
  }

  return legs.filter(leg => {
    // Check if the leg starts within the date range
    const legStartDate = new Date(leg.startTime.toISOString().split('T')[0] + 'T00:00:00Z');

    return legStartDate >= start && legStartDate <= end;
  });
}

/**
 * Formats time as HH:MM:SS without timezone info
 */
function formatTime(date) {
  const hours = date.getUTCHours();
  const minutes = date.getUTCMinutes().toString().padStart(2, '0');
  const seconds = date.getUTCSeconds().toString().padStart(2, '0');
  return `${hours}:${minutes}:${seconds}`;
}

/**
 * Formats date as YYYY-MM-DD
 */
function formatDate(date) {
  const year = date.getUTCFullYear();
  const month = (date.getUTCMonth() + 1).toString().padStart(2, '0');
  const day = date.getUTCDate().toString().padStart(2, '0');
  return `${year}-${month}-${day}`;
}

/**
 * Converts flight legs to CSV format
 */
function legsToCSV(legs) {
  const header = 'Date,Start Time (UTC),End Time (UTC)';
  const rows = legs.map(leg => {
    const date = formatDate(leg.startTime);
    const startTime = formatTime(leg.startTime);
    const endTime = formatTime(leg.endTime);
    return `${date},${startTime},${endTime}`;
  });

  return [header, ...rows].join('\n');
}

/**
 * Gets array of dates between start and end (inclusive)
 */
function getDateRange(startDate, endDate) {
  const dates = [];
  const current = new Date(startDate + 'T00:00:00Z');
  const end = new Date((endDate || startDate) + 'T00:00:00Z');

  while (current <= end) {
    const year = current.getUTCFullYear();
    const month = (current.getUTCMonth() + 1).toString().padStart(2, '0');
    const day = current.getUTCDate().toString().padStart(2, '0');
    dates.push(`${year}-${month}-${day}`);

    current.setUTCDate(current.getUTCDate() + 1);
  }

  return dates;
}

// CLI interface
async function main() {
  const args = process.argv.slice(2);

  if (args.length === 0 || args.includes('--help') || args.includes('-h')) {
    console.log(`
Usage: node flights-to-csv.js <ICAO> <START_DATE> [END_DATE]

Fetch flight legs from ADS-B Exchange and output as CSV.

Arguments:
  ICAO          The ICAO hex code of the aircraft (e.g., ad3c55)
  START_DATE    Start date in YYYY-MM-DD format
  END_DATE      Optional end date in YYYY-MM-DD format (defaults to same as start date)

Example:
  node flights-to-csv.js ad3c55 2025-12-16
  node flights-to-csv.js ad3c55 2025-08-15 2025-09-06

Note: The script will fetch data for each date in the range using the showTrace parameter.
      This allows access to historical data beyond the most recent 24-48 hours.
`);
    process.exit(0);
  }

  const icao = args[0];
  const startDate = args[1];
  const endDate = args[2] || null;

  if (!icao || !startDate) {
    console.error('Error: ICAO and START_DATE are required');
    process.exit(1);
  }

  // Get all dates in the range
  const dates = getDateRange(startDate, endDate);

  console.error(`Fetching data for ${dates.length} date(s)...`);

  let allLegs = [];

  // Fetch data for each date
  for (const date of dates) {
    console.error(`Fetching ${date}...`);

    try {
      const traceData = await fetchTraceData(icao, date);
      const legs = parseFlightLegs(traceData);
      const filteredLegs = filterLegsByDateRange(legs, date, date);

      console.error(`  Found ${filteredLegs.length} leg(s) on ${date}`);

      allLegs = allLegs.concat(filteredLegs);
    } catch (error) {
      console.error(`  Error fetching ${date}: ${error.message}`);
    }
  }

  if (allLegs.length === 0) {
    console.error('\nNo flights found in the specified date range');
    process.exit(1);
  }

  // Remove duplicates (same start time)
  const uniqueLegs = [];
  const seen = new Set();

  for (const leg of allLegs) {
    const key = leg.startTime.toISOString();
    if (!seen.has(key)) {
      seen.add(key);
      uniqueLegs.push(leg);
    }
  }

  // Sort by start time
  uniqueLegs.sort((a, b) => a.startTime - b.startTime);

  console.error(`\nTotal: ${uniqueLegs.length} unique flight leg(s)\n`);

  // Output CSV to stdout
  console.log(legsToCSV(uniqueLegs));
}

if (require.main === module) {
  main().catch(error => {
    console.error('Error:', error.message);
    process.exit(1);
  });
}

module.exports = { fetchTraceData, parseFlightLegs, filterLegsByDateRange, legsToCSV };
