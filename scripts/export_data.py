"""Export flight and telemetry data to Parquet files for public access."""

import os
from pathlib import Path

import pandas as pd
from sqlalchemy import create_engine, text

from src.config import get_database_url

DATA_DIR = Path(__file__).resolve().parent.parent / "data"


def export():
    DATA_DIR.mkdir(exist_ok=True)
    engine = create_engine(get_database_url())

    flights_df = pd.read_sql(
        text("""
            SELECT
                f.id,
                f.icao,
                f.start_time,
                f.end_time,
                f.end_time - f.start_time AS duration,
                COUNT(t.id) AS telemetry_points
            FROM flights f
            LEFT JOIN flight_telemetry t ON t.flight_id = f.id
            GROUP BY f.id
            ORDER BY f.start_time
        """),
        engine,
    )
    flights_df.to_parquet(DATA_DIR / "flights.parquet", index=False)
    print(f"Exported {len(flights_df)} flights")

    telemetry_df = pd.read_sql(
        text("""
            SELECT
                t.flight_id,
                t.latitude,
                t.longitude,
                t.altitude,
                t.ground_speed,
                t.timestamp,
                f.icao
            FROM flight_telemetry t
            JOIN flights f ON f.id = t.flight_id
            ORDER BY t.timestamp
        """),
        engine,
    )
    telemetry_df.to_parquet(DATA_DIR / "telemetry.parquet", index=False)
    print(f"Exported {len(telemetry_df)} telemetry points")


if __name__ == "__main__":
    export()
