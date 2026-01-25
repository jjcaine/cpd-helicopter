"""Tests for scraper module."""

import pytest
from datetime import datetime, timezone, timedelta

from src.scraper import (
    parse_flight_legs,
    parse_telemetry_point,
    filter_legs_by_date_range,
    get_date_range,
    get_yesterday,
)


class TestParseFlightLegs:
    """Tests for parse_flight_legs function."""

    def test_empty_trace(self):
        """Empty trace data should return empty list."""
        trace_data = {'timestamp': 1700000000, 'trace': []}
        legs = parse_flight_legs(trace_data)
        assert legs == []

    def test_missing_trace(self):
        """Missing trace key should return empty list."""
        trace_data = {'timestamp': 1700000000}
        legs = parse_flight_legs(trace_data)
        assert legs == []

    def test_single_point(self):
        """Single point should create one leg."""
        trace_data = {
            'timestamp': 1700000000,
            'trace': [[0, 41.8, -87.6, 1000]]  # [offset, lat, lon, alt]
        }
        legs = parse_flight_legs(trace_data)
        assert len(legs) == 1
        assert legs[0]['start_time'] == legs[0]['end_time']

    def test_continuous_flight(self):
        """Continuous flight (no gaps) should be one leg."""
        trace_data = {
            'timestamp': 1700000000,
            'trace': [
                [0, 41.8, -87.6, 1000],
                [60, 41.81, -87.61, 1500],    # 1 minute later
                [120, 41.82, -87.62, 2000],   # 2 minutes later
                [180, 41.83, -87.63, 2500],   # 3 minutes later
            ]
        }
        legs = parse_flight_legs(trace_data)
        assert len(legs) == 1
        assert legs[0]['start_time'] == datetime.fromtimestamp(1700000000, tz=timezone.utc)
        assert legs[0]['end_time'] == datetime.fromtimestamp(1700000180, tz=timezone.utc)

    def test_two_legs_with_gap(self):
        """Flight with >5 minute gap should create two legs."""
        trace_data = {
            'timestamp': 1700000000,
            'trace': [
                [0, 41.8, -87.6, 1000],
                [60, 41.81, -87.61, 1500],
                [120, 41.82, -87.62, 2000],
                # 10 minute gap (600 seconds)
                [720, 41.9, -87.7, 1000],
                [780, 41.91, -87.71, 1500],
            ]
        }
        legs = parse_flight_legs(trace_data)
        assert len(legs) == 2

        # First leg: 0 to 120 seconds
        assert legs[0]['start_time'] == datetime.fromtimestamp(1700000000, tz=timezone.utc)
        assert legs[0]['end_time'] == datetime.fromtimestamp(1700000120, tz=timezone.utc)

        # Second leg: 720 to 780 seconds
        assert legs[1]['start_time'] == datetime.fromtimestamp(1700000720, tz=timezone.utc)
        assert legs[1]['end_time'] == datetime.fromtimestamp(1700000780, tz=timezone.utc)

    def test_custom_gap_threshold(self):
        """Custom gap threshold should be respected."""
        trace_data = {
            'timestamp': 1700000000,
            'trace': [
                [0, 41.8, -87.6, 1000],
                [120, 41.81, -87.61, 1500],   # 2 minute gap
                [240, 41.82, -87.62, 2000],   # 2 minute gap
            ]
        }
        # Default threshold (300s = 5min) - should be one leg
        legs = parse_flight_legs(trace_data, gap_threshold=300)
        assert len(legs) == 1

        # Lower threshold (60s = 1min) - should be three legs
        legs = parse_flight_legs(trace_data, gap_threshold=60)
        assert len(legs) == 3

    def test_multiple_legs(self):
        """Multiple gaps should create multiple legs."""
        trace_data = {
            'timestamp': 1700000000,
            'trace': [
                [0, 41.8, -87.6, 1000],
                [60, 41.81, -87.61, 1500],
                # Gap 1
                [600, 41.9, -87.7, 1000],
                [660, 41.91, -87.71, 1500],
                # Gap 2
                [1500, 42.0, -87.8, 1000],
                [1560, 42.01, -87.81, 1500],
            ]
        }
        legs = parse_flight_legs(trace_data)
        assert len(legs) == 3


class TestFilterLegsByDateRange:
    """Tests for filter_legs_by_date_range function."""

    def test_filter_single_date(self):
        """Filter legs for a single date."""
        legs = [
            {
                'start_time': datetime(2025, 12, 15, 10, 0, 0, tzinfo=timezone.utc),
                'end_time': datetime(2025, 12, 15, 12, 0, 0, tzinfo=timezone.utc)
            },
            {
                'start_time': datetime(2025, 12, 16, 10, 0, 0, tzinfo=timezone.utc),
                'end_time': datetime(2025, 12, 16, 12, 0, 0, tzinfo=timezone.utc)
            },
            {
                'start_time': datetime(2025, 12, 17, 10, 0, 0, tzinfo=timezone.utc),
                'end_time': datetime(2025, 12, 17, 12, 0, 0, tzinfo=timezone.utc)
            },
        ]

        filtered = filter_legs_by_date_range(legs, '2025-12-16')
        assert len(filtered) == 1
        assert filtered[0]['start_time'].day == 16

    def test_filter_date_range(self):
        """Filter legs for a date range."""
        legs = [
            {
                'start_time': datetime(2025, 12, 15, 10, 0, 0, tzinfo=timezone.utc),
                'end_time': datetime(2025, 12, 15, 12, 0, 0, tzinfo=timezone.utc)
            },
            {
                'start_time': datetime(2025, 12, 16, 10, 0, 0, tzinfo=timezone.utc),
                'end_time': datetime(2025, 12, 16, 12, 0, 0, tzinfo=timezone.utc)
            },
            {
                'start_time': datetime(2025, 12, 17, 10, 0, 0, tzinfo=timezone.utc),
                'end_time': datetime(2025, 12, 17, 12, 0, 0, tzinfo=timezone.utc)
            },
        ]

        filtered = filter_legs_by_date_range(legs, '2025-12-15', '2025-12-16')
        assert len(filtered) == 2

    def test_filter_empty_result(self):
        """Filter with no matching legs returns empty list."""
        legs = [
            {
                'start_time': datetime(2025, 12, 15, 10, 0, 0, tzinfo=timezone.utc),
                'end_time': datetime(2025, 12, 15, 12, 0, 0, tzinfo=timezone.utc)
            },
        ]

        filtered = filter_legs_by_date_range(legs, '2025-12-20')
        assert len(filtered) == 0


class TestGetDateRange:
    """Tests for get_date_range function."""

    def test_single_date(self):
        """Single date should return list with one date."""
        dates = get_date_range('2025-12-16')
        assert dates == ['2025-12-16']

    def test_date_range(self):
        """Date range should return all dates inclusive."""
        dates = get_date_range('2025-12-14', '2025-12-17')
        assert dates == ['2025-12-14', '2025-12-15', '2025-12-16', '2025-12-17']

    def test_same_start_end(self):
        """Same start and end date should return single date."""
        dates = get_date_range('2025-12-16', '2025-12-16')
        assert dates == ['2025-12-16']

    def test_month_boundary(self):
        """Date range crossing month boundary."""
        dates = get_date_range('2025-12-30', '2026-01-02')
        assert dates == ['2025-12-30', '2025-12-31', '2026-01-01', '2026-01-02']


class TestGetYesterday:
    """Tests for get_yesterday function."""

    def test_returns_string(self):
        """Should return a string in YYYY-MM-DD format."""
        result = get_yesterday()
        assert isinstance(result, str)
        assert len(result) == 10
        assert result[4] == '-'
        assert result[7] == '-'

    def test_is_yesterday(self):
        """Should return yesterday's date."""
        result = get_yesterday()
        expected = (datetime.now(timezone.utc) - timedelta(days=1)).strftime('%Y-%m-%d')
        assert result == expected


class TestParseTelemetryPoint:
    """Tests for parse_telemetry_point function."""

    def test_parse_basic_point(self):
        """Parse a basic telemetry point."""
        base_timestamp = 1700000000
        point = [60, 41.8, -87.6, 1500, 120.5, 90.0]

        result = parse_telemetry_point(base_timestamp, point)

        assert result['timestamp'] == datetime.fromtimestamp(1700000060, tz=timezone.utc)
        assert result['latitude'] == 41.8
        assert result['longitude'] == -87.6
        assert result['altitude'] == 1500
        assert result['altitude_ground'] is False
        assert result['ground_speed'] == 120.5
        assert result['track'] == 90.0

    def test_parse_ground_altitude(self):
        """Parse point with 'ground' altitude."""
        base_timestamp = 1700000000
        point = [0, 41.8, -87.6, "ground", 0.0, 0.0]

        result = parse_telemetry_point(base_timestamp, point)

        assert result['altitude'] is None
        assert result['altitude_ground'] is True

    def test_parse_full_point(self):
        """Parse point with all fields."""
        base_timestamp = 1700000000
        # indices: 0=offset, 1=lat, 2=lon, 3=alt, 4=gs, 5=track, 6=flags,
        #          7=vrate, 8=unused, 9=unused, 10=geoalt, 11=geovrate, 12=ias, 13=roll
        point = [0, 41.8, -87.6, 2000, 150.0, 180.0, 1, 500, None, None, 2100, 480, 145, 5.5]

        result = parse_telemetry_point(base_timestamp, point)

        assert result['altitude'] == 2000
        assert result['ground_speed'] == 150.0
        assert result['track'] == 180.0
        assert result['flags'] == 1
        assert result['vertical_rate'] == 500
        assert result['geo_altitude'] == 2100
        assert result['geo_vertical_rate'] == 480
        assert result['ias'] == 145
        assert result['roll_angle'] == 5.5

    def test_parse_missing_fields(self):
        """Parse point with missing optional fields."""
        base_timestamp = 1700000000
        point = [0, 41.8, -87.6]  # Only required fields

        result = parse_telemetry_point(base_timestamp, point)

        assert result['latitude'] == 41.8
        assert result['longitude'] == -87.6
        assert result['altitude'] is None
        assert result['ground_speed'] is None
        assert result['track'] is None

    def test_parse_null_values(self):
        """Parse point with null values in array."""
        base_timestamp = 1700000000
        point = [0, 41.8, -87.6, None, None, 90.0]

        result = parse_telemetry_point(base_timestamp, point)

        assert result['altitude'] is None
        assert result['ground_speed'] is None
        assert result['track'] == 90.0


class TestParseFlightLegsWithTelemetry:
    """Tests for telemetry in parse_flight_legs."""

    def test_legs_include_telemetry(self):
        """Flight legs should include telemetry points."""
        trace_data = {
            'timestamp': 1700000000,
            'trace': [
                [0, 41.8, -87.6, 1000, 100.0, 90.0],
                [60, 41.81, -87.61, 1500, 110.0, 92.0],
                [120, 41.82, -87.62, 2000, 115.0, 95.0],
            ]
        }

        legs = parse_flight_legs(trace_data)

        assert len(legs) == 1
        assert 'telemetry' in legs[0]
        assert len(legs[0]['telemetry']) == 3
        assert legs[0]['telemetry'][0]['latitude'] == 41.8
        assert legs[0]['telemetry'][1]['altitude'] == 1500
        assert legs[0]['telemetry'][2]['ground_speed'] == 115.0

    def test_multiple_legs_separate_telemetry(self):
        """Each leg should have its own telemetry."""
        trace_data = {
            'timestamp': 1700000000,
            'trace': [
                [0, 41.8, -87.6, 1000],
                [60, 41.81, -87.61, 1500],
                # 10 minute gap
                [660, 42.0, -88.0, 2000],
                [720, 42.01, -88.01, 2500],
            ]
        }

        legs = parse_flight_legs(trace_data)

        assert len(legs) == 2
        assert len(legs[0]['telemetry']) == 2
        assert len(legs[1]['telemetry']) == 2

        # First leg telemetry
        assert legs[0]['telemetry'][0]['latitude'] == 41.8
        assert legs[0]['telemetry'][1]['altitude'] == 1500

        # Second leg telemetry
        assert legs[1]['telemetry'][0]['latitude'] == 42.0
        assert legs[1]['telemetry'][1]['altitude'] == 2500

    def test_empty_trace_no_telemetry(self):
        """Empty trace should return empty list."""
        trace_data = {'timestamp': 1700000000, 'trace': []}
        legs = parse_flight_legs(trace_data)
        assert legs == []

    def test_single_point_telemetry(self):
        """Single point should have one telemetry point."""
        trace_data = {
            'timestamp': 1700000000,
            'trace': [[0, 41.8, -87.6, 1000]]
        }

        legs = parse_flight_legs(trace_data)

        assert len(legs) == 1
        assert len(legs[0]['telemetry']) == 1
        assert legs[0]['telemetry'][0]['latitude'] == 41.8
