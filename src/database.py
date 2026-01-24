"""Database connection and session management."""

from datetime import datetime
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.sql import func

from config import get_database_url
from src.models import Base, Flight

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
