"""Tests for database module."""

import pytest
from datetime import datetime, timezone, timedelta

from src.database import SessionLocal, upsert_flight, upsert_flights
from src.models import Flight


@pytest.fixture
def db_session():
    """Create a database session for testing."""
    session = SessionLocal()
    yield session
    session.close()


@pytest.fixture
def cleanup_test_flights(db_session):
    """Clean up test flights after each test."""
    yield
    # Delete any test flights (using a test ICAO that won't conflict)
    db_session.query(Flight).filter(Flight.icao.like('test%')).delete()
    db_session.commit()


class TestUpsertFlight:
    """Tests for upsert_flight function."""

    def test_insert_new_flight(self, db_session, cleanup_test_flights):
        """Inserting a new flight should work."""
        start_time = datetime(2025, 1, 1, 10, 0, 0, tzinfo=timezone.utc)
        end_time = datetime(2025, 1, 1, 12, 0, 0, tzinfo=timezone.utc)

        result = upsert_flight(db_session, 'test01', start_time, end_time)

        assert result['action'] == 'inserted'
        assert result['flight'].icao == 'test01'
        assert result['flight'].start_time == start_time
        assert result['flight'].end_time == end_time

    def test_update_end_time_later(self, db_session, cleanup_test_flights):
        """Upserting with later end_time should update."""
        start_time = datetime(2025, 1, 2, 10, 0, 0, tzinfo=timezone.utc)
        end_time_1 = datetime(2025, 1, 2, 11, 0, 0, tzinfo=timezone.utc)
        end_time_2 = datetime(2025, 1, 2, 12, 0, 0, tzinfo=timezone.utc)

        # First insert
        result1 = upsert_flight(db_session, 'test02', start_time, end_time_1)
        assert result1['action'] == 'inserted'

        # Second upsert with later end time
        result2 = upsert_flight(db_session, 'test02', start_time, end_time_2)
        assert result2['action'] == 'updated'
        assert result2['flight'].end_time == end_time_2

    def test_no_update_same_end_time(self, db_session, cleanup_test_flights):
        """Upserting with same end_time should not update."""
        start_time = datetime(2025, 1, 3, 10, 0, 0, tzinfo=timezone.utc)
        end_time = datetime(2025, 1, 3, 12, 0, 0, tzinfo=timezone.utc)

        # First insert
        result1 = upsert_flight(db_session, 'test03', start_time, end_time)
        assert result1['action'] == 'inserted'

        # Second upsert with same end time
        result2 = upsert_flight(db_session, 'test03', start_time, end_time)
        assert result2['action'] == 'unchanged'

    def test_no_update_earlier_end_time(self, db_session, cleanup_test_flights):
        """Upserting with earlier end_time should not update."""
        start_time = datetime(2025, 1, 4, 10, 0, 0, tzinfo=timezone.utc)
        end_time_1 = datetime(2025, 1, 4, 12, 0, 0, tzinfo=timezone.utc)
        end_time_2 = datetime(2025, 1, 4, 11, 0, 0, tzinfo=timezone.utc)

        # First insert with later end time
        result1 = upsert_flight(db_session, 'test04', start_time, end_time_1)
        assert result1['action'] == 'inserted'

        # Second upsert with earlier end time - should not update
        result2 = upsert_flight(db_session, 'test04', start_time, end_time_2)
        assert result2['action'] == 'unchanged'
        assert result2['flight'].end_time == end_time_1  # Original end time preserved


class TestUpsertFlights:
    """Tests for upsert_flights batch function."""

    def test_batch_insert(self, db_session, cleanup_test_flights):
        """Batch inserting multiple flights."""
        flights = [
            {
                'icao': 'test05',
                'start_time': datetime(2025, 1, 5, 10, 0, 0, tzinfo=timezone.utc),
                'end_time': datetime(2025, 1, 5, 12, 0, 0, tzinfo=timezone.utc)
            },
            {
                'icao': 'test05',
                'start_time': datetime(2025, 1, 5, 14, 0, 0, tzinfo=timezone.utc),
                'end_time': datetime(2025, 1, 5, 16, 0, 0, tzinfo=timezone.utc)
            },
        ]

        results = upsert_flights(db_session, flights)
        assert results['inserted'] == 2
        assert results['updated'] == 0
        assert results['unchanged'] == 0

    def test_batch_mixed_operations(self, db_session, cleanup_test_flights):
        """Batch with mix of insert, update, unchanged."""
        start_time_1 = datetime(2025, 1, 6, 10, 0, 0, tzinfo=timezone.utc)
        start_time_2 = datetime(2025, 1, 6, 14, 0, 0, tzinfo=timezone.utc)

        # First batch - insert both
        flights_1 = [
            {
                'icao': 'test06',
                'start_time': start_time_1,
                'end_time': datetime(2025, 1, 6, 11, 0, 0, tzinfo=timezone.utc)
            },
            {
                'icao': 'test06',
                'start_time': start_time_2,
                'end_time': datetime(2025, 1, 6, 15, 0, 0, tzinfo=timezone.utc)
            },
        ]
        results_1 = upsert_flights(db_session, flights_1)
        assert results_1['inserted'] == 2

        # Second batch - one update (later end), one unchanged (same end), one new insert
        flights_2 = [
            {
                'icao': 'test06',
                'start_time': start_time_1,
                'end_time': datetime(2025, 1, 6, 12, 0, 0, tzinfo=timezone.utc)  # Later
            },
            {
                'icao': 'test06',
                'start_time': start_time_2,
                'end_time': datetime(2025, 1, 6, 15, 0, 0, tzinfo=timezone.utc)  # Same
            },
            {
                'icao': 'test06',
                'start_time': datetime(2025, 1, 6, 18, 0, 0, tzinfo=timezone.utc),
                'end_time': datetime(2025, 1, 6, 20, 0, 0, tzinfo=timezone.utc)  # New
            },
        ]
        results_2 = upsert_flights(db_session, flights_2)
        assert results_2['inserted'] == 1
        assert results_2['updated'] == 1
        assert results_2['unchanged'] == 1
