"""Tests for overlap clustering in scripts/dedupe_overlapping_flights.py."""

from datetime import datetime, timezone
from types import SimpleNamespace

from scripts.dedupe_overlapping_flights import find_overlap_clusters


def f(id, icao, start, end):
    return SimpleNamespace(id=id, icao=icao, start_time=start, end_time=end)


def t(day, h, m, s=0, us=0):
    return datetime(2026, 10, day, h, m, s, us, tzinfo=timezone.utc)


def test_real_cross_source_duplicate_is_clustered():
    flights = [
        f(975, 'ad389e', t(27, 22, 52, 52, 390000), t(27, 23, 59, 58, 450000)),
        f(976, 'ad389e', t(27, 22, 53, 43, 660000), t(27, 23, 59, 59, 460000)),
        f(977, 'ad389e', t(28, 0, 0, 2, 770000), t(28, 1, 12, 4)),  # midnight split: separate
    ]
    clusters = find_overlap_clusters(flights)
    assert [[x.id for x in c] for c in clusters] == [[975, 976]]


def test_no_overlap_across_aircraft():
    flights = [
        f(1, 'ad389e', t(26, 17, 0), t(26, 18, 0)),
        f(2, 'ad3c55', t(26, 17, 0), t(26, 18, 0)),
    ]
    assert find_overlap_clusters(flights) == []


def test_chain_of_overlaps_is_one_cluster():
    flights = [
        f(1, 'a', t(26, 10, 0), t(26, 11, 0)),
        f(2, 'a', t(26, 10, 30), t(26, 12, 0)),
        f(3, 'a', t(26, 11, 30), t(26, 12, 30)),
        f(4, 'a', t(26, 14, 0), t(26, 15, 0)),
    ]
    assert [[x.id for x in c] for c in find_overlap_clusters(flights)] == [[1, 2, 3]]
