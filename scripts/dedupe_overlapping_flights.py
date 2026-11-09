"""Find and merge duplicate flights: same aircraft, overlapping time ranges.

Duplicates can appear when a day is re-fetched from a different trace source
(ADS-B Exchange vs adsb.lol), which sees the same flight's first position
seconds to a minute apart. Distinct legs never overlap (they are split on
>= 5 min gaps), and midnight-split pieces are adjacent, not overlapping.

For each overlapping cluster, the lowest-id flight is kept (stable identity),
its end_time is extended to the cluster's latest end, and the other flights and
their telemetry are deleted.

Dry run by default:
    PYTHONPATH=. uv run python scripts/dedupe_overlapping_flights.py
Apply:
    PYTHONPATH=. uv run python scripts/dedupe_overlapping_flights.py --apply
"""

import argparse
import sys

from sqlalchemy import func

from src.database import SessionLocal, delete_flight_telemetry
from src.models import Flight, FlightTelemetry


def find_overlap_clusters(flights):
    """Group flights (sorted by icao, start) into clusters of overlapping [start, end] ranges."""
    clusters = []
    current = []
    current_end = None
    for f in flights:
        if current and f.icao == current[0].icao and f.start_time <= current_end:
            current.append(f)
            current_end = max(current_end, f.end_time)
        else:
            if len(current) > 1:
                clusters.append(current)
            current = [f]
            current_end = f.end_time
    if len(current) > 1:
        clusters.append(current)
    return clusters


def main():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument('--apply', action='store_true', help='Delete duplicates (default: dry run)')
    args = parser.parse_args()

    session = SessionLocal()
    try:
        flights = session.query(Flight).order_by(Flight.icao, Flight.start_time).all()
        clusters = find_overlap_clusters(flights)
        if not clusters:
            print('No overlapping flights found.')
            return

        def points(flight_id):
            return session.query(func.count(FlightTelemetry.id)).filter_by(flight_id=flight_id).scalar()

        for cluster in clusters:
            keep = min(cluster, key=lambda f: f.id)
            print(f'{keep.icao}: keep #{keep.id}')
            for f in cluster:
                marker = 'KEEP  ' if f is keep else 'DELETE'
                print(f'  {marker} #{f.id} {f.start_time} -> {f.end_time} ({points(f.id)} pts)')

        dupes = sum(len(c) - 1 for c in clusters)
        print(f'\n{len(clusters)} cluster(s), {dupes} duplicate flight(s).')

        if not args.apply:
            print('Dry run: nothing changed. Re-run with --apply to merge.')
            return

        for cluster in clusters:
            keep = min(cluster, key=lambda f: f.id)
            latest_end = max(f.end_time for f in cluster)
            if latest_end > keep.end_time:
                keep.end_time = latest_end
                keep.updated_at = func.now()
            for f in cluster:
                if f is not keep:
                    delete_flight_telemetry(session, f.id)
                    session.delete(f)
        session.commit()
        print(f'Applied: deleted {dupes} duplicate flight(s).')
    finally:
        session.close()


if __name__ == '__main__':
    sys.exit(main())
