"""Historical trace source: adsb.lol daily globe_history archives.

ADS-B Exchange gated its historical traces behind a Cloudflare Turnstile human
check in late October 2026. adsb.lol publishes the same readsb ``globe_history``
data as free daily GitHub releases (ODbL 1.0):

    https://github.com/adsblol/globe_history_<YYYY>/releases/tag/v<YYYY.MM.DD>-planes-readsb-prod-0

Each release is a ~4 GB tar split into ``.tar.aa``, ``.tar.ab``, ... parts. We
stream the parts through ``tar`` and keep only the trace files for the tracked
aircraft, so nothing large is written to disk. One download serves every
aircraft for that day; results are cached per process.

Data (c) adsb.lol contributors, licensed under ODbL 1.0.
"""

from __future__ import annotations

import gzip
import json
import os
import subprocess
import sys
import tempfile
import urllib.error
import urllib.request
from pathlib import Path
from typing import Iterable, Optional

from src.config import TRACKED_AIRCRAFT
from src.scraper import TraceSourceError

REPO_TEMPLATE = 'adsblol/globe_history_{year}'
TAG_TEMPLATE = 'v{date}-planes-readsb-prod-0'  # date as YYYY.MM.DD
CHUNK_SIZE = 1024 * 1024
HTTP_TIMEOUT = 120

# date (YYYY-MM-DD) -> {icao: trace dict}; absent icao means no trace that day
_day_cache: dict[str, dict[str, dict]] = {}


class ReleaseNotPublishedError(ValueError):
    """The archive for this date isn't published yet (or never was).

    Subclasses ValueError so callers treat it like "no trace data": the day is
    skipped and retried on the next auto-backfill run. A prolonged outage is
    caught by the ingestion stall check instead.
    """


def release_coordinates(date: str) -> tuple[str, str]:
    """Return (repo, tag) for a YYYY-MM-DD date."""
    year, month, day = date.split('-')
    return REPO_TEMPLATE.format(year=year), TAG_TEMPLATE.format(date=f'{year}.{month}.{day}')


def trace_member_pattern(icao: str) -> str:
    """tar wildcard for an aircraft's full-day trace (sharded by last 2 hex chars)."""
    icao = icao.lower()
    return f'*traces/{icao[-2:]}/trace_full_{icao}.json'


def decode_trace_file(raw: bytes) -> dict:
    """Trace files are gzip-compressed JSON despite the .json name; accept both."""
    if raw[:2] == b'\x1f\x8b':
        raw = gzip.decompress(raw)
    return json.loads(raw)


def _request(url: str, accept: str) -> urllib.request.Request:
    headers = {'Accept': accept, 'User-Agent': 'cpd-helicopter (github.com/jjcaine/cpd-helicopter)'}
    token = os.environ.get('GITHUB_TOKEN') or os.environ.get('GH_TOKEN')
    if token and 'api.github.com' in url:
        headers['Authorization'] = f'Bearer {token}'
    return urllib.request.Request(url, headers=headers)


def list_archive_parts(repo: str, tag: str) -> list[str]:
    """Return the download URLs of the tar parts for a release, in order."""
    url = f'https://api.github.com/repos/{repo}/releases/tags/{tag}'
    try:
        with urllib.request.urlopen(_request(url, 'application/vnd.github+json'), timeout=HTTP_TIMEOUT) as resp:
            release = json.load(resp)
    except urllib.error.HTTPError as e:
        if e.code == 404:
            raise ReleaseNotPublishedError(f'No adsb.lol release {repo}@{tag}') from e
        raise TraceSourceError(f'GitHub API returned HTTP {e.code} for {url}') from e
    except urllib.error.URLError as e:
        raise TraceSourceError(f'Could not reach GitHub API for {url}: {e.reason}') from e

    parts = sorted(
        (a['name'], a['browser_download_url'])
        for a in release.get('assets', [])
        if '.tar' in a['name']
    )
    if not parts:
        raise TraceSourceError(f'adsb.lol release {repo}@{tag} has no tar assets')
    return [u for _, u in parts]


def _parse_tar_stderr(stderr: str) -> list[str]:
    """Return tar error lines that aren't just 'pattern not found' (= aircraft didn't fly)."""
    real = []
    for line in stderr.splitlines():
        line = line.strip()
        if not line or 'Not found in archive' in line or 'Exiting with failure status due to previous errors' in line:
            continue
        real.append(line)
    return real


def download_day_traces(date: str, icaos: Iterable[str]) -> dict[str, dict]:
    """Stream one day's archive and return {icao: trace} for the aircraft that have a trace."""
    icaos = sorted({i.lower() for i in icaos})
    repo, tag = release_coordinates(date)
    part_urls = list_archive_parts(repo, tag)
    print(f'    Streaming adsb.lol {tag} ({len(part_urls)} part(s))...', file=sys.stderr)

    with tempfile.TemporaryDirectory(prefix='adsblol-') as tmp, tempfile.TemporaryFile() as errf:
        proc = subprocess.Popen(
            ['tar', '-x', '-C', tmp, '--wildcards', *[trace_member_pattern(i) for i in icaos]],
            stdin=subprocess.PIPE,
            stderr=errf,
        )
        assert proc.stdin is not None
        try:
            for url in part_urls:
                try:
                    with urllib.request.urlopen(_request(url, 'application/octet-stream'), timeout=HTTP_TIMEOUT) as resp:
                        while chunk := resp.read(CHUNK_SIZE):
                            proc.stdin.write(chunk)
                except urllib.error.HTTPError as e:
                    raise TraceSourceError(f'Archive download returned HTTP {e.code} for {url}') from e
                except urllib.error.URLError as e:
                    raise TraceSourceError(f'Archive download failed for {url}: {e.reason}') from e
            proc.stdin.close()
            proc.wait()
            errf.seek(0)
            stderr = errf.read().decode(errors='replace')
        except BaseException:
            proc.kill()
            proc.wait()
            raise

        errors = _parse_tar_stderr(stderr)
        if proc.returncode not in (0, 2) or errors:
            raise TraceSourceError(f'tar failed on {tag} (exit {proc.returncode}): {"; ".join(errors)[:500]}')

        traces = {}
        for icao in icaos:
            matches = list(Path(tmp).glob(f'**/traces/{icao[-2:]}/trace_full_{icao}.json'))
            if matches:
                traces[icao] = decode_trace_file(matches[0].read_bytes())
        return traces


async def fetch_trace_data(icao: str, date: Optional[str] = None) -> dict:
    """
    Fetch an aircraft's full-day trace for a UTC date (YYYY-MM-DD).

    Drop-in replacement for the former Playwright/ADS-B Exchange fetcher:
    returns readsb trace JSON ({'icao', 'timestamp', 'trace': [...], ...}).

    Raises:
        ValueError: no trace for that aircraft/day, or the day isn't published yet
        TraceSourceError: the source could not be read
    """
    if not date:
        raise ValueError('adsb.lol archives are per-day; a date is required')
    icao = icao.lower()

    if date not in _day_cache:
        wanted = set(TRACKED_AIRCRAFT) | {icao}
        _day_cache[date] = download_day_traces(date, wanted)
    elif icao not in _day_cache[date] and icao not in TRACKED_AIRCRAFT:
        _day_cache[date].update(download_day_traces(date, [icao]))

    trace = _day_cache[date].get(icao)
    if not trace or 'trace' not in trace:
        raise ValueError('No trace data found')
    return trace
