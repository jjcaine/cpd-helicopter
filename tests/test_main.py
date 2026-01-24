"""Tests for main module."""

import pytest
import tempfile
import os
from datetime import datetime, timezone

from src.main import extract_icao_from_filename, parse_csv_file


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
