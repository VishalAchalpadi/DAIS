from __future__ import annotations

from concurrent.futures import ProcessPoolExecutor

import polars as pl

from dais.parsers.base import Parser, RawInput, register_parser
from dais.spec.models import FixedWidthColumn, ParserConfig


def _parse_lines(raw_bytes: bytes, columns: list[FixedWidthColumn]) -> list[dict[str, str]]:
    """Module-level (not a method) so it's picklable for ProcessPoolExecutor."""
    text = raw_bytes.decode("utf-8")
    lines = [line for line in text.splitlines() if line.strip() != ""]
    return [
        {col.name: line[col.start - 1 : col.end].strip() for col in columns} for line in lines
    ]


def _rows_to_dataframe(rows: list[dict[str, str]], columns: list[FixedWidthColumn]) -> pl.DataFrame:
    column_names = [col.name for col in columns]
    if not rows:
        return pl.DataFrame({name: [] for name in column_names}, schema={n: pl.Utf8 for n in column_names})
    return pl.DataFrame(rows, schema={n: pl.Utf8 for n in column_names})


def _detect_line_length(raw_bytes: bytes, columns: list[FixedWidthColumn]) -> int:
    record_length = max(col.end for col in columns)
    newline_idx = raw_bytes.find(b"\n")
    if newline_idx == -1:
        terminator_len = 0
    elif newline_idx > 0 and raw_bytes[newline_idx - 1 : newline_idx] == b"\r":
        terminator_len = 2
    else:
        terminator_len = 1
    return record_length + terminator_len


@register_parser("fixed_width")
class FixedWidthParser(Parser):
    """Splits each line into fields by 1-indexed inclusive start/end
    byte-offsets defined in `parser.columns`. Pure slicing - no casting."""

    def parse(self, source: RawInput, config: ParserConfig) -> pl.DataFrame:
        if not config.columns:
            raise ValueError("fixed_width parser requires parser.columns")
        raw_bytes = self._as_bytes(source)
        rows = _parse_lines(raw_bytes, config.columns)
        return _rows_to_dataframe(rows, config.columns)


def parse_fixed_width_chunked(
    source: RawInput,
    config: ParserConfig,
    *,
    chunk_size_bytes: int = 5_000_000,
    max_workers: int = 4,
) -> pl.DataFrame:
    """For large fixed-width files: splits the file into byte-range
    chunks aligned to record boundaries and parses them in parallel
    processes, then combines into one DataFrame preserving row order.

    All chunks land before any DQ check runs - the combined result is
    what gets validated as ONE batch; callers must never validate a
    chunk independently when integrity_mode is strict.
    """
    if not config.columns:
        raise ValueError("fixed_width parser requires parser.columns")
    columns = config.columns

    raw_bytes = source if isinstance(source, bytes) else Parser._as_bytes(source)
    line_length = _detect_line_length(raw_bytes, columns)

    if line_length <= 0 or len(raw_bytes) <= chunk_size_bytes:
        rows = _parse_lines(raw_bytes, columns)
        return _rows_to_dataframe(rows, columns)

    lines_per_chunk = max(1, chunk_size_bytes // line_length)
    chunk_byte_size = lines_per_chunk * line_length

    byte_chunks: list[bytes] = []
    offset = 0
    while offset < len(raw_bytes):
        end = min(offset + chunk_byte_size, len(raw_bytes))
        byte_chunks.append(raw_bytes[offset:end])
        offset = end

    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        chunk_results = list(executor.map(_parse_lines, byte_chunks, [columns] * len(byte_chunks)))

    all_rows = [row for chunk_rows in chunk_results for row in chunk_rows]
    return _rows_to_dataframe(all_rows, columns)
