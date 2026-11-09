"""Trace parsing helpers (readsb trace format) and source error types."""

from datetime import datetime, timezone, timedelta
from typing import Optional


class TraceSourceError(RuntimeError):
    """The trace source refused or failed the request (auth gate, rate limit, outage).

    Distinct from ValueError('No trace data found'), which means the aircraft
    simply has no trace for that day. Callers must not treat this as "no flights".
    """


def parse_telemetry_point(base_timestamp: int, point: list) -> dict:
    """
    Parse a single trace point into a telemetry dict.

    ADS-B Exchange trace array indices:
        0: time offset from base timestamp (seconds)
        1: latitude
        2: longitude
        3: altitude (barometric, feet) or "ground"
        4: ground_speed (knots)
        5: track (degrees)
        6: flags (bitfield)
        7: vertical_rate (fpm)
        8: (unused/alert)
        9: (unused/spi)
        10: geo_altitude (feet)
        11: geo_vertical_rate (fpm)
        12: ias (knots)
        13: roll_angle (degrees)

    Args:
        base_timestamp: Base Unix timestamp
        point: Raw trace point array

    Returns:
        dict: Parsed telemetry point
    """
    def safe_get(arr, idx, default=None):
        """Safely get array element, returning default if missing or None."""
        if idx < len(arr):
            val = arr[idx]
            return val if val is not None else default
        return default

    # Parse altitude - can be "ground" string or integer
    raw_altitude = safe_get(point, 3)
    altitude = None
    altitude_ground = False
    if raw_altitude == "ground":
        altitude_ground = True
    elif isinstance(raw_altitude, (int, float)):
        altitude = int(raw_altitude)

    return {
        'timestamp': datetime.fromtimestamp(
            base_timestamp + point[0], tz=timezone.utc
        ),
        'latitude': point[1],
        'longitude': point[2],
        'altitude': altitude,
        'altitude_ground': altitude_ground,
        'ground_speed': safe_get(point, 4),
        'track': safe_get(point, 5),
        'flags': safe_get(point, 6),
        'vertical_rate': safe_get(point, 7),
        'geo_altitude': safe_get(point, 10),
        'geo_vertical_rate': safe_get(point, 11),
        'ias': safe_get(point, 12),
        'roll_angle': safe_get(point, 13),
    }


def parse_flight_legs(trace_data: dict, gap_threshold: int = 300) -> list[dict]:
    """
    Parse trace data into individual flight legs with telemetry.

    Args:
        trace_data: Raw trace data from ADS-B Exchange
        gap_threshold: Minimum gap in seconds to consider a new leg (default 300 = 5 min)

    Returns:
        list: Array of flight legs with start_time, end_time, and telemetry array
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
            # Extract telemetry for this leg
            telemetry = [
                parse_telemetry_point(base_timestamp, trace[j])
                for j in range(current_leg['start_index'], current_leg['end_index'] + 1)
            ]
            legs.append({
                'start_time': datetime.fromtimestamp(
                    base_timestamp + trace[current_leg['start_index']][0],
                    tz=timezone.utc
                ),
                'end_time': datetime.fromtimestamp(
                    base_timestamp + trace[current_leg['end_index']][0],
                    tz=timezone.utc
                ),
                'telemetry': telemetry
            })
            current_leg = {'start_index': i, 'end_index': i}
        else:
            current_leg['end_index'] = i

    # Don't forget the last leg
    if current_leg['start_index'] < len(trace):
        telemetry = [
            parse_telemetry_point(base_timestamp, trace[j])
            for j in range(current_leg['start_index'], current_leg['end_index'] + 1)
        ]
        legs.append({
            'start_time': datetime.fromtimestamp(
                base_timestamp + trace[current_leg['start_index']][0],
                tz=timezone.utc
            ),
            'end_time': datetime.fromtimestamp(
                base_timestamp + trace[current_leg['end_index']][0],
                tz=timezone.utc
            ),
            'telemetry': telemetry
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
