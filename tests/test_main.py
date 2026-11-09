"""Tests for main module."""

import pytest
import tempfile
import os
from datetime import date, datetime, timezone

import asyncio

import src.main as main_module
from src.main import (
    extract_icao_from_filename,
    parse_csv_file,
    compute_missing_dates,
    check_ingestion_stalled,
    compute_backfill_last_date,
    IngestionStalledError,
)
from src.scraper import TraceSourceError


class TestExtractIcaoFromFilename:
    """Tests for extract_icao_from_filename function."""

    def test_standard_format(self):
        """Standard format with ICAO prefix."""
        assert extract_icao_from_filename('ad389e_flights.csv') == 'ad389e'

    def test_uppercase_icao(self):
        """Uppercase ICAO should be lowercased."""
        assert extract_icao_from_filename('AD389E_flights.csv') == 'ad389e'

    def test_no_icao(self):
        """Filename without ICAO prefix."""
        assert extract_icao_from_filename('flights.csv') is None

    def test_short_prefix(self):
        """Prefix shorter than 6 chars should not match."""
        assert extract_icao_from_filename('ad389_flights.csv') is None

    def test_non_hex_prefix(self):
        """Non-hex prefix should not match."""
        assert extract_icao_from_filename('ghijkl_flights.csv') is None


class TestParseCsvFile:
    """Tests for parse_csv_file function."""

    def test_parse_standard_csv(self):
        """Parse standard CSV format."""
        csv_content = """Date,Start Time (UTC),End Time (UTC)
2025-12-16,18:43:54,20:05:07
2025-12-16,21:30:39,22:59:20
"""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            f.write(csv_content)
            f.flush()

            try:
                flights = parse_csv_file(f.name, 'ad389e')

                assert len(flights) == 2

                # First flight
                assert flights[0]['icao'] == 'ad389e'
                assert flights[0]['start_time'] == datetime(2025, 12, 16, 18, 43, 54, tzinfo=timezone.utc)
                assert flights[0]['end_time'] == datetime(2025, 12, 16, 20, 5, 7, tzinfo=timezone.utc)

                # Second flight
                assert flights[1]['icao'] == 'ad389e'
                assert flights[1]['start_time'] == datetime(2025, 12, 16, 21, 30, 39, tzinfo=timezone.utc)
                assert flights[1]['end_time'] == datetime(2025, 12, 16, 22, 59, 20, tzinfo=timezone.utc)
            finally:
                os.unlink(f.name)

    def test_parse_midnight_rollover(self):
        """Parse CSV with midnight rollover (end time < start time)."""
        csv_content = """Date,Start Time (UTC),End Time (UTC)
2025-12-16,23:30:00,0:30:00
"""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            f.write(csv_content)
            f.flush()

            try:
                flights = parse_csv_file(f.name, 'ad389e')

                assert len(flights) == 1
                assert flights[0]['start_time'] == datetime(2025, 12, 16, 23, 30, 0, tzinfo=timezone.utc)
                # End time should roll over to next day
                assert flights[0]['end_time'] == datetime(2025, 12, 17, 0, 30, 0, tzinfo=timezone.utc)
            finally:
                os.unlink(f.name)

    def test_parse_empty_csv(self):
        """Parse CSV with only header."""
        csv_content = """Date,Start Time (UTC),End Time (UTC)
"""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            f.write(csv_content)
            f.flush()

            try:
                flights = parse_csv_file(f.name, 'ad389e')
                assert len(flights) == 0
            finally:
                os.unlink(f.name)


class TestComputeMissingDates:
    """Tests for compute_missing_dates function."""

    def test_no_gap_returns_empty(self):
        """When last_date equals target_date, should return empty list."""
        last_date = date(2025, 3, 15)
        target_date = date(2025, 3, 15)

        result = compute_missing_dates(last_date, target_date)
        assert result == []

    def test_last_date_after_target_returns_empty(self):
        """When last_date is after target_date, should return empty list."""
        last_date = date(2025, 3, 20)
        target_date = date(2025, 3, 15)

        result = compute_missing_dates(last_date, target_date)
        assert result == []

    def test_single_day_gap(self):
        """Should return single date when gap is one day."""
        last_date = date(2025, 3, 14)
        target_date = date(2025, 3, 15)

        result = compute_missing_dates(last_date, target_date)
        assert result == ['2025-03-15']

    def test_multi_day_gap(self):
        """Should return all dates in the gap."""
        last_date = date(2025, 3, 10)
        target_date = date(2025, 3, 15)

        result = compute_missing_dates(last_date, target_date)
        assert result == [
            '2025-03-11',
            '2025-03-12',
            '2025-03-13',
            '2025-03-14',
            '2025-03-15',
        ]

    def test_month_boundary(self):
        """Should handle month boundaries correctly."""
        last_date = date(2025, 1, 30)
        target_date = date(2025, 2, 2)

        result = compute_missing_dates(last_date, target_date)
        assert result == [
            '2025-01-31',
            '2025-02-01',
            '2025-02-02',
        ]

    def test_year_boundary(self):
        """Should handle year boundaries correctly."""
        last_date = date(2024, 12, 30)
        target_date = date(2025, 1, 2)

        result = compute_missing_dates(last_date, target_date)
        assert result == [
            '2024-12-31',
            '2025-01-01',
            '2025-01-02',
        ]


class TestCheckIngestionStalled:
    """Tests for check_ingestion_stalled: fail loudly when the whole fleet goes silent."""

    def test_recent_flight_is_fine(self):
        check_ingestion_stalled({'a': date(2026, 11, 6), 'b': None}, date(2026, 11, 7))

    def test_one_aircraft_idle_is_fine_if_other_flew(self):
        # Aircraft rotate; one idle for weeks is normal.
        check_ingestion_stalled(
            {'a': date(2026, 9, 1), 'b': date(2026, 11, 5)}, date(2026, 11, 7)
        )

    def test_gap_at_threshold_is_fine(self):
        check_ingestion_stalled(
            {'a': date(2026, 10, 28)}, date(2026, 11, 7), threshold_days=10
        )

    def test_fleet_silent_beyond_threshold_raises(self):
        # The real Oct 26/27 -> Nov 7 2026 outage.
        with pytest.raises(IngestionStalledError, match='2026-10-27'):
            check_ingestion_stalled(
                {'ad389e': date(2026, 10, 27), 'ad3c55': date(2026, 10, 26)},
                date(2026, 11, 7),
                threshold_days=10,
            )

    def test_no_data_does_not_raise(self):
        check_ingestion_stalled({'a': None, 'b': None}, date(2026, 11, 7))


class TestFetchFlightsSourceErrors:
    """A blocked/failed source must propagate, not be logged as 'no flights'."""

    def test_source_error_propagates(self, monkeypatch):
        async def blocked(icao, date):
            raise TraceSourceError('Trace source returned HTTP 403')

        monkeypatch.setattr(main_module, 'fetch_trace_data', blocked)
        with pytest.raises(TraceSourceError):
            asyncio.run(main_module.fetch_flights_for_aircraft('ad389e', ['2026-11-07']))

    def test_no_trace_data_is_still_skipped(self, monkeypatch):
        async def empty(icao, date):
            raise ValueError('No trace data found')

        monkeypatch.setattr(main_module, 'fetch_trace_data', empty)
        result = asyncio.run(main_module.fetch_flights_for_aircraft('ad389e', ['2026-11-07']))
        assert result == []


class TestComputeBackfillLastDate:
    """The backfill window starts from the fleet's newest flight, capped to a recheck window."""

    def test_idle_aircraft_does_not_widen_window(self):
        # Real case: ad3c55 idle since 2026-10-26 must not force re-reading every day since.
        last = {'ad389e': date(2026, 11, 6), 'ad3c55': date(2026, 10, 26)}
        after = compute_backfill_last_date(last, date(2026, 11, 8), recheck_days=3)
        assert after == date(2026, 11, 5)
        assert compute_missing_dates(after, date(2026, 11, 8)) == [
            '2026-11-06', '2026-11-07', '2026-11-08',
        ]

    def test_real_gap_is_fully_backfilled(self):
        last = {'a': date(2026, 10, 26), 'b': date(2026, 10, 20)}
        after = compute_backfill_last_date(last, date(2026, 11, 8), recheck_days=3)
        assert after == date(2026, 10, 26)

    def test_always_rechecks_recent_days_even_if_up_to_date(self):
        last = {'a': date(2026, 11, 8)}
        after = compute_backfill_last_date(last, date(2026, 11, 8), recheck_days=3)
        assert compute_missing_dates(after, date(2026, 11, 8)) == [
            '2026-11-06', '2026-11-07', '2026-11-08',
        ]

    def test_no_data_returns_none(self):
        assert compute_backfill_last_date({'a': None, 'b': None}, date(2026, 11, 8)) is None
