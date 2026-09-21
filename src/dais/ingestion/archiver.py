"""Moves a processed source file into source.location.archive (see
spec/models.py's ArchiveConfig). Called only after a run SUCCEEDED and
reached its stop layer - see should_archive() - so a quarantined or failed
file stays where it is for review instead of vanishing into an archive.
"""
from __future__ import annotations

import shutil
from datetime import datetime, timezone
from pathlib import Path

from dais.resilience.connectors.s3_connector import S3Connector, parse_s3_uri
from dais.spec.models import PipelineSpec


def should_archive(spec: PipelineSpec, status: str, layer_reached: str | None, stop_after: str | None) -> bool:
    """True iff this spec opts in AND the run fully succeeded at the layer
    it was meant to stop at - a run capped early (e.g. a manual
    stop_after=raw test run) hasn't finished with the file, so it stays."""
    if spec.source.location.archive is None:
        return False
    return status == "succeeded" and layer_reached == (stop_after or spec.execution.stop_after)


def _unique_name(name: str, exists) -> str:
    """name, or name with a UTC timestamp inserted before the extension if a
    file of that name is already in the archive - never overwrite history."""
    if not exists(name):
        return name
    stamp = datetime.now(timezone.utc).strftime("%H%M%S%f")
    stem, dot, ext = name.rpartition(".")
    return f"{stem}__{stamp}.{ext}" if dot else f"{name}__{stamp}"


def archive_source_file(
    spec: PipelineSpec, file_path: str, s3: S3Connector | None = None, *, now: datetime | None = None
) -> str:
    """Moves file_path into the spec's archive location and returns where it
    went. Local: shutil.move. S3: copy (get+put) then delete - S3Connector
    has no server-side copy, and archived feed files are small."""
    archive = spec.source.location.archive
    assert archive is not None, "archive_source_file called for a spec with no source.location.archive"
    day = (now or datetime.now(timezone.utc)).strftime("%Y%m%d")

    if file_path.startswith("s3://"):
        if s3 is None:
            raise ValueError("an S3Connector is required to archive an s3:// source file")
        src_bucket, src_key = parse_s3_uri(file_path)
        dest_bucket, dest_prefix = parse_s3_uri(archive.path)
        base = dest_prefix.rstrip("/")
        folder = f"{base}/{day}" if archive.date_subdirs else base
        folder = folder.lstrip("/")
        name = _unique_name(
            src_key.rsplit("/", 1)[-1],
            lambda n: bool(s3.list_objects(dest_bucket, f"{folder}/{n}" if folder else n)),
        )
        dest_key = f"{folder}/{name}" if folder else name
        s3.put_object(dest_bucket, dest_key, s3.get_object(src_bucket, src_key))
        s3.delete_object(src_bucket, src_key)
        return f"s3://{dest_bucket}/{dest_key}"

    dest_dir = Path(archive.path) / day if archive.date_subdirs else Path(archive.path)
    dest_dir.mkdir(parents=True, exist_ok=True)
    src = Path(file_path)
    name = _unique_name(src.name, lambda n: (dest_dir / n).exists())
    dest = dest_dir / name
    shutil.move(str(src), str(dest))
    return str(dest)
