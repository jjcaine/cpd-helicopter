"""Database connection and session management."""

from datetime import date, datetime
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.sql import func

from config import get_database_url
from src.models import Base, Flight, FlightTelemetry

engine = create_engine(get_database_url())
SessionLocal = sessionmaker(bind=engine)


def init_db():
    """Create all database tables."""
    Base.metadata.create_all(bind=engine)


def get_db():
    """Get a database session (context manager)."""
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def get_last_ingested_date(session, icao: str = None) -> date | None:
    """
    Get the last date with ingested flight data.

    Args:
        session: SQLAlchemy session
        icao: Optional aircraft ICAO. If None, returns earliest last date across all tracked aircraft.

    Returns:
        date: The most recent date with flight data, or None if no data exists
    """
    from config import TRACKED_AIRCRAFT

    if icao:
        # Get last date for specific aircraft
        result = session.query(func.max(func.date(Flight.start_time))).filter(
            Flight.icao == icao
        ).scalar()
        return result

    # Get last date for each tracked aircraft, return the minimum (earliest)
    # This ensures we backfill from the earliest gap across all aircraft
    last_dates = []
    for aircraft_icao in TRACKED_AIRCRAFT:
        result = session.query(func.max(func.date(Flight.start_time))).filter(
            Flight.icao == aircraft_icao
        ).scalar()
        if result:
            last_dates.append(result)

    if not last_dates:
        return None

    return min(last_dates)


def upsert_flight(session, icao: str, start_time: datetime, end_time: datetime) -> dict:
    """
    Insert or update a flight record using UPSERT logic.

    If a flight with the same (icao, start_time) exists:
    - Update end_time only if the new end_time is later
    - Update updated_at timestamp

    Args:
        session: SQLAlchemy session
        icao: Aircraft ICAO hex code
        start_time: Flight start time (timezone-aware)
        end_time: Flight end time (timezone-aware)

    Returns:
        dict: {'action': 'inserted' | 'updated' | 'unchanged', 'flight': Flight}
    """
    # Check if flight already exists
    existing = session.query(Flight).filter_by(
        icao=icao, start_time=start_time
    ).first()

    if existing:
        # Flight exists - check if we should update
        if end_time > existing.end_time:
            # Update end_time
            existing.end_time = end_time
            existing.updated_at = func.now()
            session.commit()
            session.refresh(existing)
            return {'action': 'updated', 'flight': existing}
        else:
            # No update needed
            return {'action': 'unchanged', 'flight': existing}
    else:
        # Insert new flight
        stmt = insert(Flight).values(
            icao=icao,
            start_time=start_time,
            end_time=end_time
        )

        # Handle race condition with ON CONFLICT
        stmt = stmt.on_conflict_do_update(
            constraint='uix_icao_start_time',
            set_={
                'end_time': stmt.excluded.end_time,
                'updated_at': func.now()
            },
            where=(stmt.excluded.end_time > Flight.end_time)
        )

        session.execute(stmt)
        session.commit()

        # Fetch the inserted/updated flight
        flight = session.query(Flight).filter_by(
            icao=icao, start_time=start_time
        ).first()

        return {'action': 'inserted', 'flight': flight}


def upsert_flights(session, flights: list[dict]) -> dict:
    """
    Batch upsert multiple flights.

    Args:
        session: SQLAlchemy session
        flights: List of dicts with 'icao', 'start_time', 'end_time'

    Returns:
        dict: {'inserted': int, 'updated': int, 'unchanged': int}
    """
    results = {'inserted': 0, 'updated': 0, 'unchanged': 0}

    for flight_data in flights:
        result = upsert_flight(
            session,
            icao=flight_data['icao'],
            start_time=flight_data['start_time'],
            end_time=flight_data['end_time']
        )
        results[result['action']] += 1

    return results


def delete_flight_telemetry(session, flight_id: int) -> int:
    """
    Delete all telemetry points for a flight.

    Args:
        session: SQLAlchemy session
        flight_id: Flight ID to delete telemetry for

    Returns:
        int: Number of telemetry points deleted
    """
    deleted = session.query(FlightTelemetry).filter_by(flight_id=flight_id).delete()
    session.commit()
    return deleted


def insert_telemetry(session, flight_id: int, telemetry_points: list[dict]) -> int:
    """
    Bulk insert telemetry points for a flight.

    Deletes any existing telemetry for the flight before inserting.

    Args:
        session: SQLAlchemy session
        flight_id: Flight ID to associate telemetry with
        telemetry_points: List of telemetry point dicts

    Returns:
        int: Number of telemetry points inserted
    """
    if not telemetry_points:
        return 0

    # Delete existing telemetry for this flight (for re-runs)
    delete_flight_telemetry(session, flight_id)

    # Prepare records for bulk insert
    records = []
    for point in telemetry_points:
        records.append({
            'flight_id': flight_id,
            'timestamp': point['timestamp'],
            'latitude': point['latitude'],
            'longitude': point['longitude'],
            'altitude': point.get('altitude'),
            'altitude_ground': point.get('altitude_ground', False),
            'ground_speed': point.get('ground_speed'),
            'track': point.get('track'),
            'vertical_rate': point.get('vertical_rate'),
            'flags': point.get('flags'),
            'geo_altitude': point.get('geo_altitude'),
            'geo_vertical_rate': point.get('geo_vertical_rate'),
            'ias': point.get('ias'),
            'roll_angle': point.get('roll_angle'),
        })

    # Bulk insert
    session.bulk_insert_mappings(FlightTelemetry, records)
    session.commit()

    return len(records)


def upsert_flight_with_telemetry(
    session,
    icao: str,
    start_time: datetime,
    end_time: datetime,
    telemetry: list[dict] | None = None
) -> dict:
    """
    Insert or update a flight record with optional telemetry.

    Args:
        session: SQLAlchemy session
        icao: Aircraft ICAO hex code
        start_time: Flight start time (timezone-aware)
        end_time: Flight end time (timezone-aware)
        telemetry: Optional list of telemetry point dicts

    Returns:
        dict: {'action': str, 'flight': Flight, 'telemetry_count': int}
    """
    result = upsert_flight(session, icao, start_time, end_time)

    telemetry_count = 0
    if telemetry and result['flight']:
        telemetry_count = insert_telemetry(session, result['flight'].id, telemetry)

    return {
        'action': result['action'],
        'flight': result['flight'],
        'telemetry_count': telemetry_count
    }
