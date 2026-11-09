"""Tests for the adsb.lol trace source (no network)."""

import asyncio
import gzip
import io
import json
import tarfile
import urllib.error

import pytest

import src.trace_source as ts
from src.scraper import TraceSourceError


@pytest.fixture(autouse=True)
def clear_cache():
    ts._day_cache.clear()
    yield
    ts._day_cache.clear()


def make_trace(icao):
    return {'icao': icao, 'timestamp': 1793030400.0, 'trace': [[0.0, 41.5, -81.6, 500, 80.0, 90.0, 0, 0, None]]}


def make_archive(traces, gz=True):
    """Build an in-memory tar laid out like an adsb.lol globe_history release."""
    buf = io.BytesIO()
    with tarfile.open(fileobj=buf, mode='w') as tar:
        for icao, trace in traces.items():
            data = json.dumps(trace).encode()
            if gz:
                data = gzip.compress(data)
            info = tarfile.TarInfo(f'./traces/{icao[-2:]}/trace_full_{icao}.json')
            info.size = len(data)
            tar.addfile(info, io.BytesIO(data))
        noise = b'{}'
        info = tarfile.TarInfo('./traces/00/trace_full_a00000.json')
        info.size = len(noise)
        tar.addfile(info, io.BytesIO(noise))
    return buf.getvalue()


class FakeResponse(io.BytesIO):
    def __enter__(self):
        return self

    def __exit__(self, *a):
        self.close()


def install_fake_github(monkeypatch, archive: bytes, parts=2, release_status=200, part_status=200):
    """Serve a release (split into `parts` assets) from a fake urlopen."""
    size = -(-len(archive) // parts)
    chunks = {f'https://dl/part.tar.a{chr(97 + i)}': archive[i * size:(i + 1) * size] for i in range(parts)}
    calls = []

    def fake_urlopen(req, timeout=None):
        url = req.full_url
        calls.append(url)
        if 'api.github.com' in url:
            if release_status != 200:
                raise urllib.error.HTTPError(url, release_status, 'err', {}, None)
            # deliberately unsorted to check ordering
            assets = [{'name': u.rsplit('/', 1)[1], 'browser_download_url': u} for u in reversed(list(chunks))]
            assets.append({'name': 'README.md', 'browser_download_url': 'https://dl/README.md'})
            return FakeResponse(json.dumps({'assets': assets}).encode())
        if part_status != 200:
            raise urllib.error.HTTPError(url, part_status, 'err', {}, None)
        return FakeResponse(chunks[url])

    monkeypatch.setattr(ts.urllib.request, 'urlopen', fake_urlopen)
    return calls


class TestHelpers:
    def test_release_coordinates(self):
        assert ts.release_coordinates('2026-11-06') == (
            'adsblol/globe_history_2026', 'v2026.11.06-planes-readsb-prod-0'
        )

    def test_trace_member_pattern_shards_by_last_two_hex(self):
        assert ts.trace_member_pattern('AD3C55') == '*traces/55/trace_full_ad3c55.json'

    def test_decode_gzip_and_plain(self):
        t = make_trace('ad389e')
        assert ts.decode_trace_file(gzip.compress(json.dumps(t).encode())) == t
        assert ts.decode_trace_file(json.dumps(t).encode()) == t

    def test_tar_not_found_lines_are_not_errors(self):
        stderr = (
            'tar: *traces/55/trace_full_ad3c55.json: Not found in archive\n'
            'tar: Exiting with failure status due to previous errors\n'
        )
        assert ts._parse_tar_stderr(stderr) == []
        assert ts._parse_tar_stderr('tar: Unexpected EOF in archive\n') == ['tar: Unexpected EOF in archive']


class TestFetchTraceData:
    def test_extracts_both_aircraft_from_split_archive_with_one_download(self, monkeypatch):
        archive = make_archive({'ad389e': make_trace('ad389e'), 'ad3c55': make_trace('ad3c55')})
        calls = install_fake_github(monkeypatch, archive, parts=3)

        a = asyncio.run(ts.fetch_trace_data('ad389e', '2026-11-06'))
        b = asyncio.run(ts.fetch_trace_data('ad3c55', '2026-11-06'))

        assert a['icao'] == 'ad389e' and len(a['trace']) == 1
        assert b['icao'] == 'ad3c55'
        assert sum('api.github.com' in c for c in calls) == 1  # one release lookup per day
        assert calls[1:] == sorted(calls[1:])  # parts streamed in order

    def test_aircraft_without_trace_raises_value_error(self, monkeypatch):
        install_fake_github(monkeypatch, make_archive({'ad389e': make_trace('ad389e')}))
        with pytest.raises(ValueError, match='No trace data found'):
            asyncio.run(ts.fetch_trace_data('ad3c55', '2026-11-06'))

    def test_unpublished_release_is_a_skippable_value_error(self, monkeypatch):
        install_fake_github(monkeypatch, b'', release_status=404)
        with pytest.raises(ts.ReleaseNotPublishedError):
            asyncio.run(ts.fetch_trace_data('ad389e', '2026-11-08'))

    @pytest.mark.parametrize('status', [403, 429, 500])
    def test_api_errors_raise_source_error(self, monkeypatch, status):
        install_fake_github(monkeypatch, b'', release_status=status)
        with pytest.raises(TraceSourceError):
            asyncio.run(ts.fetch_trace_data('ad389e', '2026-11-06'))

    def test_part_download_error_raises_source_error(self, monkeypatch):
        install_fake_github(monkeypatch, make_archive({'ad389e': make_trace('ad389e')}), part_status=502)
        with pytest.raises(TraceSourceError):
            asyncio.run(ts.fetch_trace_data('ad389e', '2026-11-06'))

    def test_truncated_archive_raises_source_error(self, monkeypatch):
        archive = make_archive({'ad389e': make_trace('ad389e')})
        # Cut inside the second member's data block (headers/data are 512-byte
        # blocks), so tar hits a real mid-file EOF rather than clean padding.
        install_fake_github(monkeypatch, archive[:1700], parts=1)
        with pytest.raises(TraceSourceError):
            asyncio.run(ts.fetch_trace_data('ad389e', '2026-11-06'))
