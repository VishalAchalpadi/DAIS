from datetime import datetime, timezone
from pathlib import Path

import boto3
import pytest
import yaml
from moto import mock_aws

from dais.ingestion.archiver import archive_source_file, should_archive
from dais.resilience.connectors.s3_connector import S3Connector
from dais.spec.models import PipelineSpec

FIXTURE = Path(__file__).parent / "fixtures" / "valid_holdings_ingest.yaml"
DAY = datetime(2026, 9, 20, 12, 0, tzinfo=timezone.utc)


def _spec(*, kind="local", path="landing", archive=None, stop_after="gold"):
    data = yaml.safe_load(FIXTURE.read_text(encoding="utf-8"))
    data["execution"]["stop_after"] = stop_after
    data["source"]["location"] = {"kind": kind, "path": path, "file_pattern": "H_{date}.txt"}
    if archive is not None:
        data["source"]["location"]["archive"] = archive
    return PipelineSpec.model_validate(data)


def test_local_file_moves_into_a_dated_subdirectory(tmp_path):
    src = tmp_path / "landing" / "H_1.txt"
    src.parent.mkdir()
    src.write_text("data")
    spec = _spec(path=str(src.parent), archive={"path": str(tmp_path / "archive")})

    dest = archive_source_file(spec, str(src), now=DAY)

    assert Path(dest) == tmp_path / "archive" / "20260920" / "H_1.txt"
    assert Path(dest).read_text() == "data"
    assert not src.exists()


def test_flat_archive_when_date_subdirs_is_off(tmp_path):
    src = tmp_path / "H_1.txt"
    src.write_text("x")
    spec = _spec(path=str(tmp_path), archive={"path": str(tmp_path / "arch"), "date_subdirs": False})
    assert Path(archive_source_file(spec, str(src), now=DAY)) == tmp_path / "arch" / "H_1.txt"


def test_an_existing_archived_file_is_never_overwritten(tmp_path):
    spec = _spec(path=str(tmp_path), archive={"path": str(tmp_path / "arch")})
    for content in ("first", "second"):
        src = tmp_path / "H_1.txt"
        src.write_text(content)
        dest = archive_source_file(spec, str(src), now=DAY)
    archived = sorted(p.read_text() for p in (tmp_path / "arch" / "20260920").iterdir())
    assert archived == ["first", "second"]
    assert dest != str(tmp_path / "arch" / "20260920" / "H_1.txt")


@mock_aws
def test_s3_file_is_copied_then_deleted():
    boto3.client("s3", region_name="us-east-1").create_bucket(Bucket="fx-bucket")
    s3 = S3Connector(region_name="us-east-1")
    s3.put_object("fx-bucket", "incoming/H_1.txt", b"payload")
    spec = _spec(kind="s3", path="s3://fx-bucket/incoming/", archive={"path": "s3://fx-bucket/archive/"})

    dest = archive_source_file(spec, "s3://fx-bucket/incoming/H_1.txt", s3, now=DAY)

    assert dest == "s3://fx-bucket/archive/20260920/H_1.txt"
    assert s3.get_object("fx-bucket", "archive/20260920/H_1.txt") == b"payload"
    assert s3.list_objects("fx-bucket", "incoming/") == []


@pytest.mark.parametrize(
    "status,layer,stop_after,expected",
    [
        ("succeeded", "stage", None, True),        # ran to the spec's stop layer
        ("succeeded", "raw", None, False),         # capped early - file not finished
        ("succeeded", "raw", "raw", True),         # explicit stop_after honoured
        ("quarantined", "raw", None, False),       # left in place for review
        ("failed", None, None, False),
    ],
)
def test_should_archive_only_for_a_fully_succeeded_run(tmp_path, status, layer, stop_after, expected):
    spec = _spec(path=str(tmp_path), archive={"path": str(tmp_path / "a")}, stop_after="stage")
    assert should_archive(spec, status, layer, stop_after) is expected


def test_should_archive_is_off_without_an_archive_block(tmp_path):
    spec = _spec(path=str(tmp_path), stop_after="stage")
    assert should_archive(spec, "succeeded", "stage", None) is False


def test_archive_path_kind_must_match_location_kind(tmp_path):
    with pytest.raises(ValueError, match="s3://"):
        _spec(kind="s3", path="s3://fx-bucket/in/", archive={"path": "local/dir"})
    with pytest.raises(ValueError, match="local path"):
        _spec(kind="local", path=str(tmp_path), archive={"path": "s3://fx-bucket/arch/"})
