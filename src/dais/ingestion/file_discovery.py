"""Resolves SourceLocation.file_pattern against the real directory (local
or S3) at trigger time, and applies MultiFileSelection's policy to decide
which file(s) a pipeline run should actually process.

Discovery only ever happens when a run is explicitly triggered without an
exact file_path (see api/app.py's trigger_run) - there is no background
watcher/sensor here, by design (see the author-pipeline-spec / Phase
"process multiple files" plan: discovery-on-trigger, not directory
polling).
"""
from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

from dais.resilience.connectors.s3_connector import S3Connector, parse_s3_uri
from dais.spec.models import MultiFileSelection, SourceLocation

log = logging.getLogger(__name__)

_PATTERN_TOKEN_RE = re.compile(r"\{(\w+)\}")


class FileDiscoveryError(Exception):
    """Raised when discovery can't produce a usable file list - zero
    matches, or a matched filename that doesn't actually parse under
    filename_timestamp_format. Callers (the API layer) turn this into a
    4xx, not a 500 - it reflects the state of the input directory, not a
    bug."""


class NoMatchingFilesError(FileDiscoveryError):
    """Nothing in the location matches file_pattern. `unmatched` lists the
    files that ARE there but didn't match (e.g. a .xlsx saved where the
    pattern expects .csv) so a watcher can point at them instead of
    silently seeing an 'empty' directory."""

    def __init__(self, message: str, unmatched: list[str]):
        super().__init__(message)
        self.unmatched = unmatched


@dataclass(frozen=True)
class DiscoveredFile:
    path: str  # local path or "s3://bucket/key" - usable directly as run_pipeline's file_path
    sort_key: datetime


def compile_pattern(file_pattern: str) -> re.Pattern[str]:
    """Turns "HOLDINGS_{date}.txt" into a regex anchored on the full
    filename, with the {date} token becoming a named, non-greedy capture
    group - `^HOLDINGS_(?P<date>.+?)\\.txt$`. Callers are expected to have
    already validated (SourceLocation's model validator does this at spec
    load time) that the pattern has at most one {token}."""
    parts = _PATTERN_TOKEN_RE.split(file_pattern)
    regex_parts = [re.escape(parts[0])]
    if len(parts) > 1:
        token_name, literal_after = parts[1], parts[2]
        regex_parts.append(f"(?P<{token_name}>.+?)")
        regex_parts.append(re.escape(literal_after))
    return re.compile("^" + "".join(regex_parts) + "$")


def _local_candidates(path: str) -> list[tuple[str, str, datetime]]:
    """(filename, full_path, mtime) for every file directly in path."""
    directory = Path(path)
    results = []
    for entry in directory.iterdir():
        if entry.is_file():
            mtime = datetime.fromtimestamp(entry.stat().st_mtime, tz=timezone.utc)
            results.append((entry.name, str(entry), mtime))
    return results


def _s3_candidates(path: str, s3: S3Connector) -> list[tuple[str, str, datetime]]:
    bucket, prefix = parse_s3_uri(path if path.endswith("/") else path + "/")
    results = []
    for key, last_modified in s3.list_objects_with_last_modified(bucket, prefix):
        filename = key.rsplit("/", 1)[-1]
        if not filename:  # the prefix "directory" placeholder object itself
            continue
        results.append((filename, f"s3://{bucket}/{key}", last_modified))
    return results


def discover_files(
    location: SourceLocation, s3: S3Connector | None, *, strict: bool = True
) -> list[DiscoveredFile]:
    """Lists location.path, filters to files matching location.file_pattern,
    and computes each match's sort_key per location.multi_file.order_by.
    Raises FileDiscoveryError if nothing matches, or if order_by is
    filename_timestamp and a matched filename's token doesn't parse under
    filename_timestamp_format."""
    selection = location.multi_file
    assert selection is not None, "discover_files requires location.multi_file to be set"
    assert location.file_pattern is not None  # enforced by SourceLocation's own validator

    if location.kind == "sftp":
        raise FileDiscoveryError("SFTP discovery is not yet implemented")
    if location.kind == "local":
        candidates = _local_candidates(location.path)
    else:
        if s3 is None:
            raise ValueError("an S3Connector is required to discover files in an s3:// location")
        candidates = _s3_candidates(location.path, s3)

    pattern = compile_pattern(location.file_pattern)
    matched = [(name, full_path, mtime) for name, full_path, mtime in candidates if pattern.match(name)]
    if not matched:
        unmatched = sorted(name for name, _, _ in candidates)
        hint = f" (present but not matching: {', '.join(unmatched)})" if unmatched else ""
        raise NoMatchingFilesError(
            f"no files matching {location.file_pattern!r} found in {location.path!r}{hint}", unmatched
        )

    discovered = []
    for name, full_path, mtime in matched:
        if selection.order_by == "arrival_time":
            sort_key = mtime
        else:
            match = pattern.match(name)
            token_value = next(iter(match.groupdict().values()))
            try:
                sort_key = datetime.strptime(token_value, selection.filename_timestamp_format)
            except ValueError as exc:
                if not strict:
                    # A watcher must not let one misnamed file hide every
                    # good one - skip it loudly instead of raising.
                    log.warning(
                        "skipping %r: matched %r but token %r does not parse as %r",
                        name, location.file_pattern, token_value, selection.filename_timestamp_format,
                    )
                    continue
                raise FileDiscoveryError(
                    f"filename {name!r} matched {location.file_pattern!r} but its token "
                    f"{token_value!r} does not parse as {selection.filename_timestamp_format!r}: {exc}"
                ) from exc
        discovered.append(DiscoveredFile(path=full_path, sort_key=sort_key))
    return discovered


def select_files(files: list[DiscoveredFile], selection: MultiFileSelection) -> list[DiscoveredFile]:
    """mode="latest"/"earliest" -> the single max/min-by-sort_key file.
    mode="all" -> every file, sorted by sort_key per selection.order."""
    if selection.mode == "latest":
        return [max(files, key=lambda f: f.sort_key)]
    if selection.mode == "earliest":
        return [min(files, key=lambda f: f.sort_key)]
    return sorted(files, key=lambda f: f.sort_key, reverse=selection.order == "desc")


def resolve_files(location: SourceLocation, s3: S3Connector | None) -> list[DiscoveredFile]:
    """discover_files + select_files, the one entry point trigger_run
    (and its Dagster/CLI mirrors) need."""
    return select_files(discover_files(location, s3), location.multi_file)
