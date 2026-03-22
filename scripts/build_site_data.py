"""Build JSON data files from Parquet exports for the Astro dashboard site."""

import json
from pathlib import Path

import h3
import pandas as pd

DATA_DIR = Path(__file__).resolve().parent.parent / "data"
OUT_DIR = Path(__file__).resolve().parent.parent / "site" / "src" / "data"

EASTERN = "America/New_York"

REGISTRATION_MAP = {
    "ad389e": "N951CP",
    "ad3c55": "N952CP",
}

COST_PER_HOUR_LOW = 200
COST_PER_HOUR_HIGH = 400


def load_flights() -> pd.DataFrame:
    """Load and deduplicate flights parquet."""
    df = pd.read_parquet(DATA_DIR / "flights.parquet")

    # Deduplicate: round start_time to nearest second, group by (icao, rounded),
    # keep the row with the highest id (latest ingestion).
    df["start_rounded"] = df["start_time"].dt.round("s")
    df = df.sort_values("id").drop_duplicates(
        subset=["icao", "start_rounded"], keep="last"
    )
    df = df.drop(columns=["start_rounded"]).reset_index(drop=True)

    # Convert duration timedelta to hours
    df["duration_hours"] = df["duration"].dt.total_seconds() / 3600
    df["duration_minutes"] = df["duration"].dt.total_seconds() / 60

    # Eastern time columns
    df["start_eastern"] = df["start_time"].dt.tz_convert(EASTERN)
    df["end_eastern"] = df["end_time"].dt.tz_convert(EASTERN)
    df["date_eastern"] = df["start_eastern"].dt.date

    return df


def load_telemetry() -> pd.DataFrame:
    """Load telemetry parquet."""
    return pd.read_parquet(DATA_DIR / "telemetry.parquet")


def build_stats(flights: pd.DataFrame) -> dict:
    """Build summary statistics."""
    total_flights = len(flights)
    total_hours = float(flights["duration_hours"].sum())

    # Nights past 9 PM: flights whose end time is at or after 9 PM Eastern
    nine_pm_check = flights["end_eastern"].dt.hour >= 21
    nights_past_9pm = int(flights.loc[nine_pm_check, "date_eastern"].nunique())

    cost_low = round(total_hours * COST_PER_HOUR_LOW)
    cost_high = round(total_hours * COST_PER_HOUR_HIGH)

    dates = flights["date_eastern"]
    days_with_flights = int(dates.nunique())
    date_start = str(dates.min())
    date_end = str(dates.max())

    # Total days in the range
    total_days = (dates.max() - dates.min()).days + 1

    # Flights per week
    weeks = total_days / 7 if total_days > 0 else 1
    flights_per_week = round(total_flights / weeks, 1)

    return {
        "total_flights": total_flights,
        "total_hours": round(total_hours, 1),
        "nights_past_9pm": nights_past_9pm,
        "cost_estimate_low": cost_low,
        "cost_estimate_high": cost_high,
        "days_with_flights": days_with_flights,
        "total_days": total_days,
        "date_start": date_start,
        "date_end": date_end,
        "flights_per_week": flights_per_week,
    }


def build_calendar(flights: pd.DataFrame) -> list[dict]:
    """Build calendar data: hours and flight count per day."""
    grouped = flights.groupby("date_eastern").agg(
        hours=("duration_hours", "sum"),
        flights=("id", "count"),
    )
    result = []
    for date, row in grouped.iterrows():
        result.append({
            "date": str(date),
            "hours": round(float(row["hours"]), 1),
            "flights": int(row["flights"]),
        })
    return sorted(result, key=lambda x: x["date"])


def build_histogram(flights: pd.DataFrame) -> list[dict]:
    """Build hour-of-day histogram based on flight start/end times in Eastern."""
    hour_counts = [0] * 24

    for _, row in flights.iterrows():
        start = row["start_eastern"]
        end = row["end_eastern"]

        # Get each hour the flight was airborne during.
        # A flight from 7:15 PM to 9:30 PM contributes to hours 19, 20, 21.
        current = start.replace(minute=0, second=0, microsecond=0)
        end_floor = end.replace(minute=0, second=0, microsecond=0)

        while current <= end_floor:
            hour_counts[current.hour] += 1
            current += pd.Timedelta(hours=1)

    return [{"hour": h, "flights": c} for h, c in enumerate(hour_counts)]


def build_flights_table(flights: pd.DataFrame) -> list[dict]:
    """Build the flights table data, sorted descending by start_time."""
    rows = []
    for _, row in flights.sort_values("start_time", ascending=False).iterrows():
        start_et = row["start_eastern"]
        end_et = row["end_eastern"]

        rows.append({
            "id": int(row["id"]),
            "icao": row["icao"],
            "registration": REGISTRATION_MAP.get(row["icao"], row["icao"]),
            "date": str(row["date_eastern"]),
            "start_time": start_et.strftime("%-I:%M %p"),
            "end_time": end_et.strftime("%-I:%M %p"),
            "duration_minutes": round(float(row["duration_minutes"]), 1),
            "telemetry_points": int(row["telemetry_points"]),
        })
    return rows


def build_hex_map(telemetry: pd.DataFrame) -> dict:
    """Build H3 hexagon map from telemetry points."""
    # Drop rows with missing coordinates
    telem = telemetry.dropna(subset=["latitude", "longitude"])

    # Compute H3 cell for each point at resolution 8
    cells = telem.apply(
        lambda r: h3.latlng_to_cell(r["latitude"], r["longitude"], 8), axis=1
    )

    counts = cells.value_counts()

    hexagons = []
    for cell, count in counts.items():
        lat, lng = h3.cell_to_latlng(cell)
        hexagons.append({
            "h3_index": cell,
            "count": int(count),
            "lat": round(lat, 6),
            "lng": round(lng, 6),
        })

    return {"hexagons": hexagons}


def write_json(path: Path, data) -> None:
    """Write data as JSON."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w") as f:
        json.dump(data, f, separators=(",", ":"))


def main():
    print("Loading Parquet data...")
    flights = load_flights()
    telemetry = load_telemetry()
    print(f"  {len(flights)} flights (after dedup), {len(telemetry)} telemetry points")

    OUT_DIR.mkdir(parents=True, exist_ok=True)

    print("Building stats.json...")
    stats = build_stats(flights)
    write_json(OUT_DIR / "stats.json", stats)
    print(f"  {stats['total_flights']} flights, {stats['total_hours']} hours")

    print("Building calendar.json...")
    calendar = build_calendar(flights)
    write_json(OUT_DIR / "calendar.json", calendar)
    print(f"  {len(calendar)} days with flights")

    print("Building histogram.json...")
    histogram = build_histogram(flights)
    write_json(OUT_DIR / "histogram.json", histogram)
    peak = max(histogram, key=lambda x: x["flights"])
    print(f"  Peak hour: {peak['hour']}:00 with {peak['flights']} flights")

    print("Building flights-table.json...")
    table = build_flights_table(flights)
    write_json(OUT_DIR / "flights-table.json", table)
    print(f"  {len(table)} flight records")

    print("Building hex-map.json...")
    hex_map = build_hex_map(telemetry)
    write_json(OUT_DIR / "hex-map.json", hex_map)
    print(f"  {len(hex_map['hexagons'])} hexagons")

    print(f"\nAll files written to {OUT_DIR}/")


if __name__ == "__main__":
    main()
