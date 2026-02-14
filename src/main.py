"""Main entry point for CPD Helicopter Flight Tracker."""

import argparse
import asyncio
import csv
import re
import sys
from datetime import datetime, timezone
from pathlib import Path

from config import TRACKED_AIRCRAFT
from src.scraper import (
    fetch_trace_data,
    parse_flight_legs,
    filter_legs_by_date_range,
    get_date_range,
    get_yesterday,
)
from src.database import (
    SessionLocal, upsert_flights, upsert_flight_with_telemetry,
    get_flights_without_telemetry, insert_telemetry,
)


def parse_args():
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        description='CPD Helicopter Flight Tracker - Fetch and store flight data'
    )

    parser.add_argument(
        '--icao',
        type=str,
        help='Aircraft ICAO hex code (e.g., ad389e). If not specified, uses all tracked aircraft.'
    )
    parser.add_argument(
        '--start-date',
        type=str,
        help='Start date in YYYY-MM-DD format'
    )
    parser.add_argument(
        '--end-date',
        type=str,
        help='End date in YYYY-MM-DD format (defaults to start-date if not specified)'
    )
    parser.add_argument(
        '--yesterday',
        action='store_true',
        help='Fetch flights for yesterday (UTC)'
    )
    parser.add_argument(
        '--backfill',
        type=str,
        metavar='CSV_FILE',
        help='Backfill flights from a CSV file'
    )
    parser.add_argument(
        '--no-telemetry',
        action='store_true',
        help='Skip telemetry capture (only store flight start/end times)'
    )
    parser.add_argument(
        '--backfill-telemetry',
        action='store_true',
        help='Backfill telemetry for existing flights that have no telemetry data'
    )

    return parser.parse_args()


def extract_icao_from_filename(filename: str) -> str | None:
    """
    Extract ICAO code from filename if present.

    Examples:
        'ad389e_flights.csv' -> 'ad389e'
        'flights.csv' -> None
    """
    match = re.match(r'^([a-f0-9]{6})_', filename.lower())
    return match.group(1) if match else None


def parse_csv_file(filepath: str, icao: str) -> list[dict]:
    """
    Parse a CSV file and return flight records.

    Expected CSV format:
        Date,Start Time (UTC),End Time (UTC)
        2025-12-16,18:43:54,20:05:07

    Args:
        filepath: Path to the CSV file
        icao: Aircraft ICAO code

    Returns:
        list: List of flight dicts with 'icao', 'start_time', 'end_time'
    """
    flights = []

    with open(filepath, 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            date_str = row['Date']
            start_time_str = row['Start Time (UTC)']
            end_time_str = row['End Time (UTC)']

            # Parse start time
            start_time = datetime.strptime(
                f"{date_str} {start_time_str}",
                '%Y-%m-%d %H:%M:%S'
            ).replace(tzinfo=timezone.utc)

            # Parse end time - handle midnight rollover
            end_time = datetime.strptime(
                f"{date_str} {end_time_str}",
                '%Y-%m-%d %H:%M:%S'
            ).replace(tzinfo=timezone.utc)

            # If end time is before start time, it rolled over to the next day
            if end_time < start_time:
                end_time = end_time.replace(day=end_time.day + 1)

            flights.append({
                'icao': icao,
                'start_time': start_time,
                'end_time': end_time
            })

    return flights


def backfill_from_csv(filepath: str, icao: str | None = None):
    """
    Backfill flights from a CSV file into the database.

    Args:
        filepath: Path to the CSV file
        icao: Aircraft ICAO code (optional, will try to extract from filename)
    """
    path = Path(filepath)

    if not path.exists():
        print(f"Error: File not found: {filepath}", file=sys.stderr)
        sys.exit(1)

    # Try to extract ICAO from filename if not provided
    if not icao:
        icao = extract_icao_from_filename(path.name)
        if not icao:
            print("Error: Could not determine ICAO code. Please provide --icao argument.", file=sys.stderr)
            sys.exit(1)
        print(f"Detected ICAO from filename: {icao}")

    print(f"Backfilling from {filepath} for aircraft {icao}...")

    flights = parse_csv_file(filepath, icao)
    print(f"Parsed {len(flights)} flights from CSV")

    if not flights:
        print("No flights to import")
        return

    session = SessionLocal()
    try:
        results = upsert_flights(session, flights)
        print(f"Results: {results['inserted']} inserted, {results['updated']} updated, {results['unchanged']} unchanged")
    finally:
        session.close()


async def fetch_flights_for_aircraft(icao: str, dates: list[str], include_telemetry: bool = True) -> list[dict]:
    """
    Fetch all flights for an aircraft across multiple dates.

    Args:
        icao: Aircraft ICAO code
        dates: List of dates in YYYY-MM-DD format
        include_telemetry: Whether to include telemetry data

    Returns:
        list: List of flight dicts with 'icao', 'start_time', 'end_time', and optionally 'telemetry'
    """
    all_flights = []
    seen_start_times = set()

    for date in dates:
        print(f"  Fetching {icao} for {date}...", file=sys.stderr)

        try:
            trace_data = await fetch_trace_data(icao, date)
            legs = parse_flight_legs(trace_data)
            filtered_legs = filter_legs_by_date_range(legs, date, date)

            if not filtered_legs:
                print(f"    No flights found for {icao} on {date}", file=sys.stderr)
                continue

            print(f"    Found {len(filtered_legs)} flight(s)", file=sys.stderr)

            for leg in filtered_legs:
                # Deduplicate by start time
                start_key = leg['start_time'].isoformat()
                if start_key not in seen_start_times:
                    seen_start_times.add(start_key)
                    flight_data = {
                        'icao': icao,
                        'start_time': leg['start_time'],
                        'end_time': leg['end_time']
                    }
                    if include_telemetry:
                        flight_data['telemetry'] = leg.get('telemetry', [])
                    all_flights.append(flight_data)

        except ValueError as e:
            print(f"    Error fetching {icao} on {date}: {e}", file=sys.stderr)
        except Exception as e:
            print(f"    Unexpected error fetching {icao} on {date}: {e}", file=sys.stderr)

    return all_flights


async def sync_flights(
    aircraft: list[str],
    start_date: str,
    end_date: str | None = None,
    include_telemetry: bool = True
):
    """
    Sync flights for aircraft over a date range.

    Args:
        aircraft: List of ICAO codes
        start_date: Start date in YYYY-MM-DD format
        end_date: End date in YYYY-MM-DD format (optional)
        include_telemetry: Whether to capture and store telemetry data
    """
    dates = get_date_range(start_date, end_date)
    print(f"Fetching data for {len(dates)} date(s): {start_date} to {end_date or start_date}", file=sys.stderr)
    if not include_telemetry:
        print("Telemetry capture disabled", file=sys.stderr)

    all_flights = []

    for icao in aircraft:
        print(f"\nProcessing aircraft {icao}...", file=sys.stderr)
        flights = await fetch_flights_for_aircraft(icao, dates, include_telemetry)
        all_flights.extend(flights)

    if not all_flights:
        print("\nNo flights found in the specified date range", file=sys.stderr)
        return

    # Sort by start time
    all_flights.sort(key=lambda f: f['start_time'])

    print(f"\nTotal: {len(all_flights)} flight(s) to sync", file=sys.stderr)

    # Upsert to database with telemetry
    session = SessionLocal()
    try:
        results = {'inserted': 0, 'updated': 0, 'unchanged': 0}
        total_telemetry = 0

        for flight_data in all_flights:
            telemetry = flight_data.get('telemetry') if include_telemetry else None
            result = upsert_flight_with_telemetry(
                session,
                icao=flight_data['icao'],
                start_time=flight_data['start_time'],
                end_time=flight_data['end_time'],
                telemetry=telemetry
            )
            results[result['action']] += 1
            total_telemetry += result['telemetry_count']

        print(f"Results: {results['inserted']} inserted, {results['updated']} updated, {results['unchanged']} unchanged", file=sys.stderr)
        if include_telemetry:
            print(f"Telemetry: {total_telemetry} points stored", file=sys.stderr)
    finally:
        session.close()


async def backfill_telemetry(
    icao: str | None = None,
    start_date: str | None = None,
    end_date: str | None = None,
):
    """
    Backfill telemetry for existing flights that have no telemetry data.

    Args:
        icao: Optional ICAO filter
        start_date: Optional start date filter (YYYY-MM-DD)
        end_date: Optional end date filter (YYYY-MM-DD)
    """
    session = SessionLocal()
    try:
        flights = get_flights_without_telemetry(session, icao, start_date, end_date)

        if not flights:
            print("No flights without telemetry found.", file=sys.stderr)
            return

        print(f"Found {len(flights)} flight(s) without telemetry", file=sys.stderr)

        # Group flights by (icao, date) to minimize fetches
        groups: dict[tuple[str, str], list] = {}
        for flight in flights:
            date_str = flight.start_time.strftime('%Y-%m-%d')
            key = (flight.icao, date_str)
            groups.setdefault(key, []).append(flight)

        print(f"Grouped into {len(groups)} (aircraft, date) pair(s)", file=sys.stderr)

        total_telemetry = 0
        matched_flights = 0
        tolerance_seconds = 60

        for (group_icao, date_str), group_flights in groups.items():
            print(f"\n  Fetching {group_icao} for {date_str}...", file=sys.stderr)

            try:
                trace_data = await fetch_trace_data(group_icao, date_str)
                legs = parse_flight_legs(trace_data)
            except ValueError as e:
                print(f"    Error: {e}", file=sys.stderr)
                continue
            except Exception as e:
                print(f"    Unexpected error: {e}", file=sys.stderr)
                continue

            if not legs:
                print(f"    No legs found in trace data", file=sys.stderr)
                continue

            print(f"    Found {len(legs)} leg(s) in trace data", file=sys.stderr)

            for flight in group_flights:
                # Find the best matching leg by start_time
                best_leg = None
                best_diff = None
                for leg in legs:
                    diff = abs((leg['start_time'] - flight.start_time).total_seconds())
                    if best_diff is None or diff < best_diff:
                        best_diff = diff
                        best_leg = leg

                if best_leg and best_diff <= tolerance_seconds:
                    telemetry = best_leg.get('telemetry', [])
                    if telemetry:
                        count = insert_telemetry(session, flight.id, telemetry)
                        total_telemetry += count
                        matched_flights += 1
                        print(
                            f"    Matched flight {flight.id} "
                            f"(diff={best_diff:.0f}s): {count} points",
                            file=sys.stderr
                        )
                    else:
                        print(
                            f"    Matched flight {flight.id} but leg has no telemetry",
                            file=sys.stderr
                        )
                else:
                    diff_str = f"{best_diff:.0f}s" if best_diff is not None else "N/A"
                    print(
                        f"    No match for flight {flight.id} "
                        f"(start={flight.start_time}, closest diff={diff_str})",
                        file=sys.stderr
                    )

        print(f"\nBackfill complete: {matched_flights} flight(s) matched, "
              f"{total_telemetry} telemetry points inserted", file=sys.stderr)
    finally:
        session.close()


def main():
    """Main entry point."""
    args = parse_args()

    # Backfill telemetry mode
    if args.backfill_telemetry:
        asyncio.run(backfill_telemetry(args.icao, args.start_date, args.end_date))
        return

    # Backfill from CSV mode
    if args.backfill:
        backfill_from_csv(args.backfill, args.icao)
        return

    # Determine date range
    if args.yesterday:
        start_date = get_yesterday()
        end_date = None
    elif args.start_date:
        start_date = args.start_date
        end_date = args.end_date
    else:
        print("Error: Must specify --yesterday or --start-date", file=sys.stderr)
        sys.exit(1)

    # Determine aircraft list
    if args.icao:
        aircraft = [args.icao]
    else:
        aircraft = TRACKED_AIRCRAFT
        print(f"Using tracked aircraft: {', '.join(aircraft)}", file=sys.stderr)

    # Run async sync
    include_telemetry = not args.no_telemetry
    asyncio.run(sync_flights(aircraft, start_date, end_date, include_telemetry))


if __name__ == '__main__':
    main()
