"""Tests for telemetry model and database functions."""

import pytest
from datetime import datetime, timezone, timedelta

from src.database import (
    SessionLocal, upsert_flight, insert_telemetry,
    delete_flight_telemetry, upsert_flight_with_telemetry,
    get_flights_without_telemetry,
)
from src.models import Flight, FlightTelemetry


@pytest.fixture
def db_session():
    """Create a database session for testing."""
    session = SessionLocal()
    yield session
    session.close()


@pytest.fixture
def cleanup_test_data(db_session):
    """Clean up test flights and telemetry after each test."""
    yield
    # Delete telemetry first (due to foreign key)
    db_session.query(FlightTelemetry).filter(
        FlightTelemetry.flight_id.in_(
            db_session.query(Flight.id).filter(Flight.icao.like('telem%'))
        )
    ).delete(synchronize_session='fetch')
    # Then delete flights
    db_session.query(Flight).filter(Flight.icao.like('telem%')).delete()
    db_session.commit()


class TestInsertTelemetry:
    """Tests for insert_telemetry function."""

    def test_insert_telemetry_points(self, db_session, cleanup_test_data):
        """Insert telemetry points for a flight."""
        # First create a flight
        start_time = datetime(2025, 1, 1, 10, 0, 0, tzinfo=timezone.utc)
        end_time = datetime(2025, 1, 1, 11, 0, 0, tzinfo=timezone.utc)
        result = upsert_flight(db_session, 'telem1', start_time, end_time)
        flight_id = result['flight'].id

        # Create telemetry points
        telemetry = [
            {
                'timestamp': datetime(2025, 1, 1, 10, 0, 0, tzinfo=timezone.utc),
                'latitude': 41.8,
                'longitude': -87.6,
                'altitude': 1000,
                'ground_speed': 120.5,
                'track': 90.0,
            },
            {
                'timestamp': datetime(2025, 1, 1, 10, 0, 5, tzinfo=timezone.utc),
                'latitude': 41.81,
                'longitude': -87.61,
                'altitude': 1500,
                'ground_speed': 125.0,
                'track': 92.0,
            },
        ]

        count = insert_telemetry(db_session, flight_id, telemetry)
        assert count == 2

        # Verify telemetry was inserted
        points = db_session.query(FlightTelemetry).filter_by(flight_id=flight_id).all()
        assert len(points) == 2
        assert points[0].latitude == 41.8
        assert points[0].altitude == 1000
        assert points[1].latitude == 41.81

    def test_insert_telemetry_replaces_existing(self, db_session, cleanup_test_data):
        """Re-inserting telemetry should replace existing points."""
        # Create flight
        start_time = datetime(2025, 1, 2, 10, 0, 0, tzinfo=timezone.utc)
        end_time = datetime(2025, 1, 2, 11, 0, 0, tzinfo=timezone.utc)
        result = upsert_flight(db_session, 'telem2', start_time, end_time)
        flight_id = result['flight'].id

        # Insert first set
        telemetry1 = [
            {
                'timestamp': datetime(2025, 1, 2, 10, 0, 0, tzinfo=timezone.utc),
                'latitude': 41.8,
                'longitude': -87.6,
            },
            {
                'timestamp': datetime(2025, 1, 2, 10, 0, 5, tzinfo=timezone.utc),
                'latitude': 41.81,
                'longitude': -87.61,
            },
        ]
        insert_telemetry(db_session, flight_id, telemetry1)

        # Insert second set (should replace)
        telemetry2 = [
            {
                'timestamp': datetime(2025, 1, 2, 10, 0, 0, tzinfo=timezone.utc),
                'latitude': 42.0,
                'longitude': -88.0,
            },
        ]
        count = insert_telemetry(db_session, flight_id, telemetry2)
        assert count == 1

        # Verify only new telemetry exists
        points = db_session.query(FlightTelemetry).filter_by(flight_id=flight_id).all()
        assert len(points) == 1
        assert points[0].latitude == 42.0

    def test_insert_empty_telemetry(self, db_session, cleanup_test_data):
        """Empty telemetry list should return 0."""
        start_time = datetime(2025, 1, 3, 10, 0, 0, tzinfo=timezone.utc)
        end_time = datetime(2025, 1, 3, 11, 0, 0, tzinfo=timezone.utc)
        result = upsert_flight(db_session, 'telem3', start_time, end_time)
        flight_id = result['flight'].id

        count = insert_telemetry(db_session, flight_id, [])
        assert count == 0

    def test_insert_telemetry_with_nulls(self, db_session, cleanup_test_data):
        """Telemetry with null fields should be handled."""
        start_time = datetime(2025, 1, 4, 10, 0, 0, tzinfo=timezone.utc)
        end_time = datetime(2025, 1, 4, 11, 0, 0, tzinfo=timezone.utc)
        result = upsert_flight(db_session, 'telem4', start_time, end_time)
        flight_id = result['flight'].id

        telemetry = [
            {
                'timestamp': datetime(2025, 1, 4, 10, 0, 0, tzinfo=timezone.utc),
                'latitude': 41.8,
                'longitude': -87.6,
                'altitude': None,
                'altitude_ground': True,
                'ground_speed': None,
            },
        ]

        count = insert_telemetry(db_session, flight_id, telemetry)
        assert count == 1

        point = db_session.query(FlightTelemetry).filter_by(flight_id=flight_id).first()
        assert point.altitude is None
        assert point.altitude_ground is True
        assert point.ground_speed is None


class TestUpsertFlightWithTelemetry:
    """Tests for upsert_flight_with_telemetry function."""

    def test_insert_with_telemetry(self, db_session, cleanup_test_data):
        """Insert flight with telemetry."""
        start_time = datetime(2025, 1, 5, 10, 0, 0, tzinfo=timezone.utc)
        end_time = datetime(2025, 1, 5, 11, 0, 0, tzinfo=timezone.utc)
        telemetry = [
            {
                'timestamp': datetime(2025, 1, 5, 10, 0, 0, tzinfo=timezone.utc),
                'latitude': 41.8,
                'longitude': -87.6,
            },
            {
                'timestamp': datetime(2025, 1, 5, 10, 30, 0, tzinfo=timezone.utc),
                'latitude': 41.9,
                'longitude': -87.7,
            },
        ]

        result = upsert_flight_with_telemetry(
            db_session, 'telem5', start_time, end_time, telemetry
        )

        assert result['action'] == 'inserted'
        assert result['telemetry_count'] == 2

    def test_insert_without_telemetry(self, db_session, cleanup_test_data):
        """Insert flight without telemetry."""
        start_time = datetime(2025, 1, 6, 10, 0, 0, tzinfo=timezone.utc)
        end_time = datetime(2025, 1, 6, 11, 0, 0, tzinfo=timezone.utc)

        result = upsert_flight_with_telemetry(
            db_session, 'telem6', start_time, end_time, None
        )

        assert result['action'] == 'inserted'
        assert result['telemetry_count'] == 0

    def test_update_replaces_telemetry(self, db_session, cleanup_test_data):
        """Updating a flight should replace its telemetry."""
        start_time = datetime(2025, 1, 7, 10, 0, 0, tzinfo=timezone.utc)
        end_time1 = datetime(2025, 1, 7, 11, 0, 0, tzinfo=timezone.utc)
        end_time2 = datetime(2025, 1, 7, 12, 0, 0, tzinfo=timezone.utc)

        telemetry1 = [
            {
                'timestamp': datetime(2025, 1, 7, 10, 0, 0, tzinfo=timezone.utc),
                'latitude': 41.8,
                'longitude': -87.6,
            },
        ]
        telemetry2 = [
            {
                'timestamp': datetime(2025, 1, 7, 10, 0, 0, tzinfo=timezone.utc),
                'latitude': 42.0,
                'longitude': -88.0,
            },
            {
                'timestamp': datetime(2025, 1, 7, 11, 30, 0, tzinfo=timezone.utc),
                'latitude': 42.1,
                'longitude': -88.1,
            },
        ]

        # First insert
        result1 = upsert_flight_with_telemetry(
            db_session, 'telem7', start_time, end_time1, telemetry1
        )
        assert result1['telemetry_count'] == 1
        flight_id = result1['flight'].id

        # Update with new end time and telemetry
        result2 = upsert_flight_with_telemetry(
            db_session, 'telem7', start_time, end_time2, telemetry2
        )
        assert result2['action'] == 'updated'
        assert result2['telemetry_count'] == 2

        # Verify telemetry was replaced
        points = db_session.query(FlightTelemetry).filter_by(flight_id=flight_id).all()
        assert len(points) == 2
        assert points[0].latitude == 42.0


class TestDeleteFlightTelemetry:
    """Tests for delete_flight_telemetry function."""

    def test_delete_telemetry(self, db_session, cleanup_test_data):
        """Delete telemetry for a flight."""
        start_time = datetime(2025, 1, 8, 10, 0, 0, tzinfo=timezone.utc)
        end_time = datetime(2025, 1, 8, 11, 0, 0, tzinfo=timezone.utc)

        result = upsert_flight_with_telemetry(
            db_session, 'telem8', start_time, end_time,
            [
                {
                    'timestamp': datetime(2025, 1, 8, 10, 0, 0, tzinfo=timezone.utc),
                    'latitude': 41.8,
                    'longitude': -87.6,
                },
            ]
        )
        flight_id = result['flight'].id

        deleted = delete_flight_telemetry(db_session, flight_id)
        assert deleted == 1

        # Verify telemetry was deleted
        points = db_session.query(FlightTelemetry).filter_by(flight_id=flight_id).all()
        assert len(points) == 0


class TestFlightTelemetryRelationship:
    """Tests for Flight-FlightTelemetry relationship."""

    def test_flight_has_telemetry(self, db_session, cleanup_test_data):
        """Flight should have telemetry relationship."""
        start_time = datetime(2025, 1, 9, 10, 0, 0, tzinfo=timezone.utc)
        end_time = datetime(2025, 1, 9, 11, 0, 0, tzinfo=timezone.utc)

        result = upsert_flight_with_telemetry(
            db_session, 'telem9', start_time, end_time,
            [
                {
                    'timestamp': datetime(2025, 1, 9, 10, 0, 0, tzinfo=timezone.utc),
                    'latitude': 41.8,
                    'longitude': -87.6,
                },
                {
                    'timestamp': datetime(2025, 1, 9, 10, 30, 0, tzinfo=timezone.utc),
                    'latitude': 41.9,
                    'longitude': -87.7,
                },
            ]
        )

        # Query flight and access telemetry via relationship
        flight = db_session.query(Flight).filter_by(id=result['flight'].id).first()
        assert len(flight.telemetry) == 2
        assert flight.telemetry[0].latitude == 41.8

    def test_cascade_delete(self, db_session, cleanup_test_data):
        """Deleting flight should cascade delete telemetry."""
        start_time = datetime(2025, 1, 10, 10, 0, 0, tzinfo=timezone.utc)
        end_time = datetime(2025, 1, 10, 11, 0, 0, tzinfo=timezone.utc)

        result = upsert_flight_with_telemetry(
            db_session, 'telema', start_time, end_time,
            [
                {
                    'timestamp': datetime(2025, 1, 10, 10, 0, 0, tzinfo=timezone.utc),
                    'latitude': 41.8,
                    'longitude': -87.6,
                },
            ]
        )
        flight_id = result['flight'].id

        # Delete the flight using session.delete() to trigger ORM cascade
        flight = db_session.query(Flight).filter_by(id=flight_id).first()
        db_session.delete(flight)
        db_session.commit()

        # Verify telemetry was cascade deleted
        points = db_session.query(FlightTelemetry).filter_by(flight_id=flight_id).all()
        assert len(points) == 0


class TestGetFlightsWithoutTelemetry:
    """Tests for get_flights_without_telemetry function."""

    def test_returns_flights_without_telemetry(self, db_session, cleanup_test_data):
        """Flights with no telemetry should be returned."""
        start_time = datetime(2025, 2, 1, 10, 0, 0, tzinfo=timezone.utc)
        end_time = datetime(2025, 2, 1, 11, 0, 0, tzinfo=timezone.utc)
        upsert_flight(db_session, 'telemb', start_time, end_time)

        flights = get_flights_without_telemetry(db_session)
        icaos = [f.icao for f in flights]
        assert 'telemb' in icaos

    def test_excludes_flights_with_telemetry(self, db_session, cleanup_test_data):
        """Flights with telemetry should not be returned."""
        start_time = datetime(2025, 2, 2, 10, 0, 0, tzinfo=timezone.utc)
        end_time = datetime(2025, 2, 2, 11, 0, 0, tzinfo=timezone.utc)

        # Flight with telemetry
        upsert_flight_with_telemetry(
            db_session, 'telemc', start_time, end_time,
            [{'timestamp': start_time, 'latitude': 41.8, 'longitude': -87.6}]
        )
        # Flight without telemetry
        upsert_flight(db_session, 'telemd', start_time, end_time)

        flights = get_flights_without_telemetry(db_session)
        icaos = [f.icao for f in flights]
        assert 'telemd' in icaos
        assert 'telemc' not in icaos

    def test_icao_filter(self, db_session, cleanup_test_data):
        """ICAO filter should limit results."""
        start_time = datetime(2025, 2, 3, 10, 0, 0, tzinfo=timezone.utc)
        end_time = datetime(2025, 2, 3, 11, 0, 0, tzinfo=timezone.utc)

        upsert_flight(db_session, 'teleme', start_time, end_time)
        upsert_flight(db_session, 'telemf', start_time, end_time)

        flights = get_flights_without_telemetry(db_session, icao='teleme')
        icaos = [f.icao for f in flights]
        assert 'teleme' in icaos
        assert 'telemf' not in icaos

    def test_date_range_filter(self, db_session, cleanup_test_data):
        """Date range filter should limit results."""
        # Flight on Feb 4
        upsert_flight(
            db_session, 'telemg',
            datetime(2025, 2, 4, 10, 0, 0, tzinfo=timezone.utc),
            datetime(2025, 2, 4, 11, 0, 0, tzinfo=timezone.utc),
        )
        # Flight on Feb 6
        upsert_flight(
            db_session, 'telemh',
            datetime(2025, 2, 6, 10, 0, 0, tzinfo=timezone.utc),
            datetime(2025, 2, 6, 11, 0, 0, tzinfo=timezone.utc),
        )

        flights = get_flights_without_telemetry(
            db_session, start_date='2025-02-05', end_date='2025-02-07'
        )
        icaos = [f.icao for f in flights]
        assert 'telemh' in icaos
        assert 'telemg' not in icaos

    def test_no_flights_returns_empty(self, db_session, cleanup_test_data):
        """Should return empty list when no flights match."""
        flights = get_flights_without_telemetry(
            db_session, icao='nonexistent'
        )
        assert flights == []
