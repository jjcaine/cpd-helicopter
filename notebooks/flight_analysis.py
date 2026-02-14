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
    # Dependencies — install in Colab if needed
    import os
    import pandas as pd
    import plotly.express as px
    import plotly.graph_objects as go
    from sqlalchemy import create_engine, text
    return go, os, pd, px, create_engine, text


@app.cell
def _(mo):
    mo.md(r"## Database Connection")
    return


@app.cell
def _(create_engine, os):
    # Build connection string from environment variables
    # Set these in Colab via Secrets or userdata
    _db_url = (
        f"postgresql://{os.getenv('DB_USER', 'doadmin')}"
        f":{os.getenv('DB_PASSWORD')}"
        f"@{os.getenv('DB_HOST', 'db-postgresql-nyc3-13586-do-user-12959197-0.j.db.ondigitalocean.com')}"
        f":{os.getenv('DB_PORT', '25060')}"
        f"/{os.getenv('DB_NAME', 'defaultdb')}"
        f"?sslmode={os.getenv('DB_SSLMODE', 'require')}"
    )
    engine = create_engine(_db_url)
    return (engine,)


@app.cell
def _(mo):
    mo.md(r"## Load Flight Data")
    return


@app.cell
def _(engine, pd, text):
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
    flights_df["start_time"] = pd.to_datetime(flights_df["start_time"])
    flights_df["end_time"] = pd.to_datetime(flights_df["end_time"])
    flights_df["date"] = flights_df["start_time"].dt.date
    flights_df["duration_minutes"] = flights_df["duration"].dt.total_seconds() / 60
    flights_df
    return (flights_df,)


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
def _(engine, pd, text):
    # Load telemetry for most recent flight with data
    telemetry_df = pd.read_sql(
        text("""
            SELECT
                t.latitude,
                t.longitude,
                t.altitude,
                t.ground_speed,
                t.timestamp,
                f.icao
            FROM flight_telemetry t
            JOIN flights f ON f.id = t.flight_id
            WHERE t.flight_id = (
                SELECT flight_id FROM flight_telemetry
                ORDER BY timestamp DESC LIMIT 1
            )
            ORDER BY t.timestamp
        """),
        engine,
    )
    telemetry_df
    return (telemetry_df,)


@app.cell
def _(px, telemetry_df):
    fig_map = px.scatter_map(
        telemetry_df,
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
