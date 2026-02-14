"""Tests for database module."""

import pytest
from datetime import datetime, timezone, timedelta, date
from unittest.mock import patch

from src.database import SessionLocal, upsert_flight, upsert_flights, get_last_ingested_date
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


class TestGetLastIngestedDate:
    """Tests for get_last_ingested_date function."""

    def test_no_data_returns_none(self, db_session, cleanup_test_flights):
        """When no flights exist for aircraft, should return None."""
        result = get_last_ingested_date(db_session, 'test99')
        assert result is None

    def test_single_aircraft_with_data(self, db_session, cleanup_test_flights):
        """Should return the latest date for a specific aircraft."""
        # Insert flights on different dates
        flights = [
            {
                'icao': 'test10',
                'start_time': datetime(2025, 3, 15, 10, 0, 0, tzinfo=timezone.utc),
                'end_time': datetime(2025, 3, 15, 12, 0, 0, tzinfo=timezone.utc)
            },
            {
                'icao': 'test10',
                'start_time': datetime(2025, 3, 20, 10, 0, 0, tzinfo=timezone.utc),
                'end_time': datetime(2025, 3, 20, 12, 0, 0, tzinfo=timezone.utc)
            },
            {
                'icao': 'test10',
                'start_time': datetime(2025, 3, 18, 10, 0, 0, tzinfo=timezone.utc),
                'end_time': datetime(2025, 3, 18, 12, 0, 0, tzinfo=timezone.utc)
            },
        ]
        upsert_flights(db_session, flights)

        result = get_last_ingested_date(db_session, 'test10')
        assert result == date(2025, 3, 20)

    def test_multiple_flights_same_date(self, db_session, cleanup_test_flights):
        """Multiple flights on the same date should return that date."""
        flights = [
            {
                'icao': 'test11',
                'start_time': datetime(2025, 4, 10, 8, 0, 0, tzinfo=timezone.utc),
                'end_time': datetime(2025, 4, 10, 10, 0, 0, tzinfo=timezone.utc)
            },
            {
                'icao': 'test11',
                'start_time': datetime(2025, 4, 10, 14, 0, 0, tzinfo=timezone.utc),
                'end_time': datetime(2025, 4, 10, 16, 0, 0, tzinfo=timezone.utc)
            },
        ]
        upsert_flights(db_session, flights)

        result = get_last_ingested_date(db_session, 'test11')
        assert result == date(2025, 4, 10)

    @patch('src.config.TRACKED_AIRCRAFT', ['test12', 'test13'])
    def test_no_icao_returns_earliest_last_date(self, db_session, cleanup_test_flights):
        """When no icao specified, should return earliest last date across tracked aircraft."""
        # test12 has data up to March 25
        flights_12 = [
            {
                'icao': 'test12',
                'start_time': datetime(2025, 3, 25, 10, 0, 0, tzinfo=timezone.utc),
                'end_time': datetime(2025, 3, 25, 12, 0, 0, tzinfo=timezone.utc)
            },
        ]
        # test13 has data up to March 20 (earlier)
        flights_13 = [
            {
                'icao': 'test13',
                'start_time': datetime(2025, 3, 20, 10, 0, 0, tzinfo=timezone.utc),
                'end_time': datetime(2025, 3, 20, 12, 0, 0, tzinfo=timezone.utc)
            },
        ]
        upsert_flights(db_session, flights_12)
        upsert_flights(db_session, flights_13)

        result = get_last_ingested_date(db_session)
        # Should return March 20 (the earliest last date)
        assert result == date(2025, 3, 20)

    @patch('src.config.TRACKED_AIRCRAFT', ['test14', 'test15'])
    def test_no_icao_with_one_aircraft_missing_data(self, db_session, cleanup_test_flights):
        """When one tracked aircraft has no data, should only consider aircraft with data."""
        # Only test14 has data
        flights = [
            {
                'icao': 'test14',
                'start_time': datetime(2025, 5, 10, 10, 0, 0, tzinfo=timezone.utc),
                'end_time': datetime(2025, 5, 10, 12, 0, 0, tzinfo=timezone.utc)
            },
        ]
        upsert_flights(db_session, flights)

        result = get_last_ingested_date(db_session)
        # Should return May 10 (the only aircraft with data)
        assert result == date(2025, 5, 10)

    @patch('src.config.TRACKED_AIRCRAFT', ['test16', 'test17'])
    def test_no_icao_all_missing_data(self, db_session, cleanup_test_flights):
        """When no tracked aircraft have data, should return None."""
        result = get_last_ingested_date(db_session)
        assert result is None
