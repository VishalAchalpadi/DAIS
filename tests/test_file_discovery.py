import os
import time

import boto3
import pytest
from moto import mock_aws

from dais.ingestion.file_discovery import (
    FileDiscoveryError,
    compile_pattern,
    discover_files,
    resolve_files,
    select_files,
)
from dais.resilience.connectors.s3_connector import S3Connector
from dais.spec.models import SourceLocation


# ---------------------------------------------------------------------------
# compile_pattern
# ---------------------------------------------------------------------------

def test_compile_pattern_matches_token_and_rejects_others():
    pattern = compile_pattern("HOLDINGS_{date}.txt")
    match = pattern.match("HOLDINGS_20260301.txt")
    assert match is not None
    assert match.group("date") == "20260301"
    assert pattern.match("OTHER_20260301.txt") is None


def test_compile_pattern_with_no_token_matches_literal_name_only():
    pattern = compile_pattern("REF_TABLE.csv")
    assert pattern.match("REF_TABLE.csv") is not None
    assert pattern.match("REF_TABLE2.csv") is None


# ---------------------------------------------------------------------------
# local discovery
# ---------------------------------------------------------------------------

def _touch(path, name, content="x", mtime_offset=None):
    p = path / name
    p.write_text(content)
    if mtime_offset is not None:
        t = time.time() + mtime_offset
        os.utime(p, (t, t))
    return p


def _location(path, mode="latest", order_by="arrival_time", fmt=None, order="asc", on_earlier_failure="continue"):
    multi_file = {"mode": mode, "order_by": order_by, "order": order, "on_earlier_failure": on_earlier_failure}
    if fmt:
        multi_file["filename_timestamp_format"] = fmt
    return SourceLocation(
        kind="local", path=str(path), file_pattern="HOLDINGS_{date}.txt", multi_file=multi_file
    )


def test_discover_files_filters_by_pattern(tmp_path):
    _touch(tmp_path, "HOLDINGS_20260301.txt")
    _touch(tmp_path, "OTHER.txt")
    loc = _location(tmp_path)
    files = discover_files(loc, None)
    assert len(files) == 1
    assert files[0].path.endswith("HOLDINGS_20260301.txt")


def test_discover_files_raises_on_zero_matches(tmp_path):
    _touch(tmp_path, "OTHER.txt")
    loc = _location(tmp_path)
    with pytest.raises(FileDiscoveryError, match="no files matching"):
        discover_files(loc, None)


def test_discover_files_raises_on_unparseable_filename_timestamp(tmp_path):
    _touch(tmp_path, "HOLDINGS_notadate.txt")
    loc = _location(tmp_path, order_by="filename_timestamp", fmt="%Y%m%d")
    with pytest.raises(FileDiscoveryError, match="does not parse"):
        discover_files(loc, None)


def test_select_latest_and_earliest_by_arrival_time(tmp_path):
    _touch(tmp_path, "HOLDINGS_A.txt", mtime_offset=-20)
    _touch(tmp_path, "HOLDINGS_B.txt", mtime_offset=-10)
    _touch(tmp_path, "HOLDINGS_C.txt", mtime_offset=0)

    latest = resolve_files(_location(tmp_path, mode="latest"), None)
    assert len(latest) == 1 and latest[0].path.endswith("HOLDINGS_C.txt")

    earliest = resolve_files(_location(tmp_path, mode="earliest"), None)
    assert len(earliest) == 1 and earliest[0].path.endswith("HOLDINGS_A.txt")


def test_select_all_by_filename_timestamp_order(tmp_path):
    _touch(tmp_path, "HOLDINGS_20260301.txt")
    _touch(tmp_path, "HOLDINGS_20260303.txt")
    _touch(tmp_path, "HOLDINGS_20260302.txt")

    asc = resolve_files(_location(tmp_path, mode="all", order_by="filename_timestamp", fmt="%Y%m%d"), None)
    assert [f.path.split(os.sep)[-1] for f in asc] == [
        "HOLDINGS_20260301.txt",
        "HOLDINGS_20260302.txt",
        "HOLDINGS_20260303.txt",
    ]

    desc = resolve_files(
        _location(tmp_path, mode="all", order_by="filename_timestamp", fmt="%Y%m%d", order="desc"), None
    )
    assert [f.path.split(os.sep)[-1] for f in desc] == [
        "HOLDINGS_20260303.txt",
        "HOLDINGS_20260302.txt",
        "HOLDINGS_20260301.txt",
    ]


def test_select_files_modes_directly():
    from dais.ingestion.file_discovery import DiscoveredFile
    from datetime import datetime, timezone

    files = [
        DiscoveredFile(path="a", sort_key=datetime(2026, 1, 1, tzinfo=timezone.utc)),
        DiscoveredFile(path="b", sort_key=datetime(2026, 1, 3, tzinfo=timezone.utc)),
        DiscoveredFile(path="c", sort_key=datetime(2026, 1, 2, tzinfo=timezone.utc)),
    ]
    from dais.spec.models import MultiFileSelection

    assert select_files(files, MultiFileSelection(mode="latest"))[0].path == "b"
    assert select_files(files, MultiFileSelection(mode="earliest"))[0].path == "a"
    assert [f.path for f in select_files(files, MultiFileSelection(mode="all"))] == ["a", "c", "b"]
    assert [f.path for f in select_files(files, MultiFileSelection(mode="all", order="desc"))] == ["b", "c", "a"]


# ---------------------------------------------------------------------------
# S3 discovery (moto-mocked, no real AWS account needed)
# ---------------------------------------------------------------------------

@mock_aws
def test_discover_files_from_s3():
    boto3.client("s3", region_name="us-east-1").create_bucket(Bucket="test-bucket")
    s3 = S3Connector(region_name="us-east-1")
    s3.put_object("test-bucket", "holdings/incoming/HOLDINGS_20260301.txt", b"x")
    s3.put_object("test-bucket", "holdings/incoming/HOLDINGS_20260302.txt", b"x")
    s3.put_object("test-bucket", "holdings/incoming/OTHER.txt", b"x")

    loc = SourceLocation(
        kind="s3",
        path="s3://test-bucket/holdings/incoming/",
        file_pattern="HOLDINGS_{date}.txt",
        multi_file={"mode": "all", "order_by": "filename_timestamp", "filename_timestamp_format": "%Y%m%d"},
    )
    resolved = resolve_files(loc, s3)
    assert [f.path for f in resolved] == [
        "s3://test-bucket/holdings/incoming/HOLDINGS_20260301.txt",
        "s3://test-bucket/holdings/incoming/HOLDINGS_20260302.txt",
    ]


def test_non_strict_discovery_skips_a_misnamed_file_instead_of_hiding_every_file(tmp_path):
    _touch(tmp_path, "HOLDINGS_20260301.txt")
    _touch(tmp_path, "HOLDINGS_notadate.txt")
    loc = _location(tmp_path, mode="all", order_by="filename_timestamp", fmt="%Y%m%d")

    with pytest.raises(FileDiscoveryError, match="does not parse"):
        discover_files(loc, None)  # on-demand triggers stay strict

    files = discover_files(loc, None, strict=False)  # the sensor does not
    assert [f.path.split(os.sep)[-1] for f in files] == ["HOLDINGS_20260301.txt"]


def test_no_match_error_names_the_files_that_were_present(tmp_path):
    from dais.ingestion.file_discovery import NoMatchingFilesError

    _touch(tmp_path, "HOLDINGS_20260301.xlsx")  # right stem, wrong extension
    with pytest.raises(NoMatchingFilesError, match="HOLDINGS_20260301.xlsx") as exc:
        discover_files(_location(tmp_path), None)
    assert exc.value.unmatched == ["HOLDINGS_20260301.xlsx"]
