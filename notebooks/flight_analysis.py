import marimo

__generated_with = "0.19.11"
app = marimo.App(width="medium")


@app.cell
def _():
    import marimo as mo
    return (mo,)


@app.cell
def _(mo):
    mo.md(
        r"""
        # CPD Helicopter Flight Analysis

        Analysis of Chicago Police Department helicopter flight data
        sourced from ADS-B Exchange.
        """
    )
    return


@app.cell
def _():
    import os
    import pandas as pd
    import plotly.express as px
    import plotly.graph_objects as go
    return go, os, pd, px


@app.cell
def _(os):
    USE_DB = os.getenv("DB_PASSWORD") is not None
    DATA_URL = "https://raw.githubusercontent.com/jjcaine/cpd-helicopter/main/data"
    return DATA_URL, USE_DB


@app.cell
def _(mo):
    mo.md(r"## Load Flight Data")
    return


@app.cell
def _(DATA_URL, USE_DB, os, pd):
    if USE_DB:
        from sqlalchemy import create_engine, text

        _db_url = (
            f"postgresql://{os.getenv('DB_USER', 'doadmin')}"
            f":{os.getenv('DB_PASSWORD')}"
            f"@{os.getenv('DB_HOST', 'db-postgresql-nyc3-13586-do-user-12959197-0.j.db.ondigitalocean.com')}"
            f":{os.getenv('DB_PORT', '25060')}"
            f"/{os.getenv('DB_NAME', 'defaultdb')}"
            f"?sslmode={os.getenv('DB_SSLMODE', 'require')}"
        )
        _engine = create_engine(_db_url)
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
            _engine,
        )
    else:
        flights_df = pd.read_parquet(f"{DATA_URL}/flights.parquet")

    flights_df["start_time"] = pd.to_datetime(flights_df["start_time"])
    flights_df["end_time"] = pd.to_datetime(flights_df["end_time"])
    flights_df["date"] = flights_df["start_time"].dt.date
    flights_df["duration_minutes"] = (
        (flights_df["end_time"] - flights_df["start_time"]).dt.total_seconds() / 60
    )
    flights_df
    return (flights_df,)


@app.cell
def _(DATA_URL, USE_DB, os, pd):
    if USE_DB:
        from sqlalchemy import create_engine as _ce
        from sqlalchemy import text as _text

        _db_url2 = (
            f"postgresql://{os.getenv('DB_USER', 'doadmin')}"
            f":{os.getenv('DB_PASSWORD')}"
            f"@{os.getenv('DB_HOST', 'db-postgresql-nyc3-13586-do-user-12959197-0.j.db.ondigitalocean.com')}"
            f":{os.getenv('DB_PORT', '25060')}"
            f"/{os.getenv('DB_NAME', 'defaultdb')}"
            f"?sslmode={os.getenv('DB_SSLMODE', 'require')}"
        )
        _engine2 = _ce(_db_url2)
        telemetry_df = pd.read_sql(
            _text("""
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
            _engine2,
        )
    else:
        telemetry_df = pd.read_parquet(f"{DATA_URL}/telemetry.parquet")

    telemetry_df
    return (telemetry_df,)


@app.cell
def _(flights_df, mo):
    mo.md(
        f"""
        ## Overview

        - **Total flights:** {len(flights_df):,}
        - **Date range:** {flights_df["date"].min()} to {flights_df["date"].max()}
        - **Aircraft tracked:** {flights_df["icao"].nunique()}
        """
    )
    return


@app.cell
def _(mo):
    mo.md(r"## Flights Per Day")
    return


@app.cell
def _(flights_df, px):
    daily_counts = flights_df.groupby("date").size().reset_index(name="flights")
    fig_daily = px.bar(
        daily_counts,
        x="date",
        y="flights",
        title="Flights Per Day",
        labels={"date": "Date", "flights": "Number of Flights"},
    )
    fig_daily
    return


@app.cell
def _(mo):
    mo.md(r"## Flight Duration Distribution")
    return


@app.cell
def _(flights_df, px):
    fig_duration = px.histogram(
        flights_df,
        x="duration_minutes",
        nbins=50,
        title="Flight Duration Distribution",
        labels={"duration_minutes": "Duration (minutes)"},
    )
    fig_duration
    return


@app.cell
def _(mo):
    mo.md(r"## Flight Telemetry Map")
    return


@app.cell
def _(px, telemetry_df):
    # Show most recent flight's telemetry
    _last_flight = telemetry_df["flight_id"].iloc[-1]
    _recent = telemetry_df[telemetry_df["flight_id"] == _last_flight]
    fig_map = px.scatter_map(
        _recent,
        lat="latitude",
        lon="longitude",
        color="altitude",
        hover_data=["ground_speed", "timestamp"],
        title="Flight Path (Most Recent)",
        zoom=10,
    )
    fig_map
    return


if __name__ == "__main__":
    app.run()
