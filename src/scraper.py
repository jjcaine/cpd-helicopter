"""ADS-B Exchange scraping logic using Playwright."""

import asyncio
from datetime import datetime, timezone, timedelta
from typing import Optional
from playwright.async_api import async_playwright


async def fetch_trace_data(icao: str, date: Optional[str] = None) -> dict:
    """
    Fetch trace data from ADS-B Exchange for a specific aircraft and date.

    Args:
        icao: Aircraft ICAO hex code (e.g., 'ad389e')
        date: Date in YYYY-MM-DD format (optional, defaults to current day)

    Returns:
        dict: Trace data containing timestamp and trace array

    Raises:
        ValueError: If no trace data is found
    """
    trace_data = None

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(
            user_agent='Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
        )
        page = await context.new_page()

        async def handle_response(response):
            nonlocal trace_data
            url = response.url
            if ('trace_full' in url or 'trace_recent' in url) and url.endswith('.json'):
                try:
                    data = await response.json()
                    # Prefer trace_full over trace_recent
                    if trace_data is None or 'trace_full' in url:
                        trace_data = data
                except Exception:
                    pass

        page.on('response', handle_response)

        # Build URL with optional showTrace parameter
        url = f'https://globe.adsbexchange.com/?icao={icao}'
        if date:
            url += f'&showTrace={date}'

        try:
            await page.goto(url, wait_until='load', timeout=15000)
        except Exception:
            pass

        # Wait for trace data to load
        await page.wait_for_timeout(5000)
        await browser.close()

    if not trace_data or 'trace' not in trace_data:
        raise ValueError('No trace data found')

    return trace_data


def parse_flight_legs(trace_data: dict, gap_threshold: int = 300) -> list[dict]:
    """
    Parse trace data into individual flight legs.

    Args:
        trace_data: Raw trace data from ADS-B Exchange
        gap_threshold: Minimum gap in seconds to consider a new leg (default 300 = 5 min)

    Returns:
        list: Array of flight legs with start_time and end_time as datetime objects
    """
    base_timestamp = trace_data.get('timestamp', 0)
    trace = trace_data.get('trace', [])

    if not trace:
        return []

    legs = []
    current_leg = {'start_index': 0, 'end_index': 0}

    for i in range(1, len(trace)):
        time_diff = trace[i][0] - trace[i - 1][0]

        # If we find a gap, save the current leg and start a new one
        if time_diff > gap_threshold:
            current_leg['end_index'] = i - 1
            legs.append({
                'start_time': datetime.fromtimestamp(
                    base_timestamp + trace[current_leg['start_index']][0],
                    tz=timezone.utc
                ),
                'end_time': datetime.fromtimestamp(
                    base_timestamp + trace[current_leg['end_index']][0],
                    tz=timezone.utc
                )
            })
            current_leg = {'start_index': i, 'end_index': i}
        else:
            current_leg['end_index'] = i

    # Don't forget the last leg
    if current_leg['start_index'] < len(trace):
        legs.append({
            'start_time': datetime.fromtimestamp(
                base_timestamp + trace[current_leg['start_index']][0],
                tz=timezone.utc
            ),
            'end_time': datetime.fromtimestamp(
                base_timestamp + trace[current_leg['end_index']][0],
                tz=timezone.utc
            )
        })

    return legs


def filter_legs_by_date_range(
    legs: list[dict],
    start_date: str,
    end_date: Optional[str] = None
) -> list[dict]:
    """
    Filter flight legs by date range.

    Args:
        legs: List of flight legs
        start_date: Start date in YYYY-MM-DD format
        end_date: End date in YYYY-MM-DD format (optional, defaults to start_date)

    Returns:
        list: Filtered flight legs
    """
    start = datetime.strptime(start_date, '%Y-%m-%d').replace(tzinfo=timezone.utc)

    if end_date:
        end = datetime.strptime(end_date, '%Y-%m-%d').replace(
            hour=23, minute=59, second=59, microsecond=999999, tzinfo=timezone.utc
        )
    else:
        end = start.replace(hour=23, minute=59, second=59, microsecond=999999)

    filtered = []
    for leg in legs:
        # Check if the leg starts within the date range
        leg_start_date = leg['start_time'].replace(
            hour=0, minute=0, second=0, microsecond=0
        )
        if start <= leg_start_date <= end:
            filtered.append(leg)

    return filtered


def get_date_range(start_date: str, end_date: Optional[str] = None) -> list[str]:
    """
    Get list of dates between start and end (inclusive).

    Args:
        start_date: Start date in YYYY-MM-DD format
        end_date: End date in YYYY-MM-DD format (optional, defaults to start_date)

    Returns:
        list: List of date strings in YYYY-MM-DD format
    """
    dates = []
    current = datetime.strptime(start_date, '%Y-%m-%d')
    end = datetime.strptime(end_date or start_date, '%Y-%m-%d')

    while current <= end:
        dates.append(current.strftime('%Y-%m-%d'))
        current += timedelta(days=1)

    return dates


def get_yesterday() -> str:
    """Get yesterday's date in YYYY-MM-DD format (UTC)."""
    yesterday = datetime.now(timezone.utc) - timedelta(days=1)
    return yesterday.strftime('%Y-%m-%d')
