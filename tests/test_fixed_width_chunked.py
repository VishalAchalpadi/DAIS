from dais.parsers import get_parser
from dais.parsers.fixed_width_parser import parse_fixed_width_chunked
from dais.spec.models import FixedWidthColumn, ParserConfig


def _config():
    return ParserConfig(
        type="fixed_width",
        columns=[
            FixedWidthColumn(name="account_id", start=1, end=5),
            FixedWidthColumn(name="qty", start=6, end=10),
        ],
    )


def _synthetic_file(n_rows: int) -> bytes:
    # True fixed-width: every record must be exactly the same byte length
    # (account_id: 5 chars, qty: 5 chars) - that invariant is what makes
    # byte-offset chunking valid at all, so the generator must honor it
    # even as `i` grows past 2 or 4 digits.
    lines = [f"A{i:04d}{i:05d}\n".encode() for i in range(n_rows)]
    return b"".join(lines)


def test_chunked_matches_unchunked_for_small_file():
    raw = _synthetic_file(20)
    config = _config()

    baseline = get_parser("fixed_width").parse(raw, config)
    chunked = parse_fixed_width_chunked(raw, config, chunk_size_bytes=5_000_000)

    assert chunked.to_dicts() == baseline.to_dicts()


def test_chunked_splits_into_multiple_chunks_and_preserves_row_order():
    raw = _synthetic_file(500)
    config = _config()

    # force many small chunks: each record is 11 bytes, so 55 bytes ~= 5 rows/chunk
    chunked = parse_fixed_width_chunked(raw, config, chunk_size_bytes=55, max_workers=4)
    baseline = get_parser("fixed_width").parse(raw, config)

    assert chunked.height == baseline.height == 500
    assert chunked.to_dicts() == baseline.to_dicts()  # order preserved across chunks


def test_chunked_handles_windows_line_endings():
    lines = [f"A{i:04d}{i:05d}\r\n".encode() for i in range(200)]
    raw = b"".join(lines)
    config = _config()

    chunked = parse_fixed_width_chunked(raw, config, chunk_size_bytes=130, max_workers=3)
    baseline = get_parser("fixed_width").parse(raw, config)

    assert chunked.to_dicts() == baseline.to_dicts()


def test_chunked_handles_chunk_size_smaller_than_one_record():
    raw = _synthetic_file(10)
    config = _config()

    # chunk_size smaller than a single record: must still parse whole records
    chunked = parse_fixed_width_chunked(raw, config, chunk_size_bytes=3, max_workers=2)

    assert chunked.height == 10
    assert chunked["account_id"][0] == "A0000"


def test_chunked_empty_file():
    config = _config()
    result = parse_fixed_width_chunked(b"", config, chunk_size_bytes=10)
    assert result.height == 0


def test_chunked_requires_columns():
    import pytest

    config = ParserConfig(type="fixed_width")
    with pytest.raises(ValueError, match="columns"):
        parse_fixed_width_chunked(b"anything", config)
