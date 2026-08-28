"""Pre-flight, whole-file checks that run BEFORE anything lands in raw.

Catches partial sends (too small), duplicate/corrupted feeds (too large),
or a grossly unexpected row count - independent of any row-level DQ rule.
A file outside range never reaches raw; it is quarantined like a strict
DQ failure (see quality/file_validator.py, added in Phase 3).
"""
from __future__ import annotations

from dataclasses import dataclass, field

from dais.spec.models import ControlGatesConfig, FixedWidthColumn, ParserConfig


@dataclass
class GateResult:
    passed: bool
    file_size_bytes: int
    row_count: int
    failures: list[str] = field(default_factory=list)


def _detect_line_terminator_len(raw_bytes: bytes) -> int:
    """Sniffs \\r\\n vs \\n from the first line so the fixed-width row-count
    arithmetic accounts for it without a full parse."""
    newline_idx = raw_bytes.find(b"\n")
    if newline_idx == -1:
        return 0  # no terminator found - single unterminated line, or empty
    if newline_idx > 0 and raw_bytes[newline_idx - 1 : newline_idx] == b"\r":
        return 2
    return 1


def compute_fixed_width_row_count(raw_bytes: bytes, columns: list[FixedWidthColumn]) -> int:
    """Exact row count from file_size / record_length - no field parsing."""
    if not raw_bytes:
        return 0
    record_length = max(col.end for col in columns)
    terminator_len = _detect_line_terminator_len(raw_bytes)
    line_length = record_length + terminator_len
    if line_length <= 0:
        return 0
    return len(raw_bytes) // line_length


def compute_approx_row_count(raw_bytes: bytes, has_header: bool = False) -> int:
    """Fast approximate row count for csv/delimited/json/xml - a single
    newline-count pass, not a real parse. Exact precision isn't required
    for this gate, only catching grossly out-of-range files."""
    if not raw_bytes:
        return 0
    count = raw_bytes.count(b"\n")
    if raw_bytes[-1:] != b"\n":
        count += 1  # last unterminated line still counts as a row
    if has_header and count > 0:
        count -= 1
    return count


def compute_row_count(raw_bytes: bytes, parser_config: ParserConfig) -> int:
    if parser_config.type == "fixed_width":
        if not parser_config.columns:
            raise ValueError("fixed_width row-count gate requires parser.columns")
        return compute_fixed_width_row_count(raw_bytes, parser_config.columns)
    has_header = bool(parser_config.has_header) if parser_config.type in ("csv", "delimited") else False
    return compute_approx_row_count(raw_bytes, has_header=has_header)


def run_control_gates(
    raw_bytes: bytes, gates: ControlGatesConfig, parser_config: ParserConfig
) -> GateResult:
    file_size = len(raw_bytes)
    row_count = compute_row_count(raw_bytes, parser_config)

    failures: list[str] = []
    if file_size < gates.file_size.min_bytes:
        failures.append(
            f"file_size {file_size} below min_bytes {gates.file_size.min_bytes}"
        )
    if file_size > gates.file_size.max_bytes:
        failures.append(
            f"file_size {file_size} above max_bytes {gates.file_size.max_bytes}"
        )
    if row_count < gates.row_count.min_rows:
        failures.append(
            f"row_count {row_count} below min_rows {gates.row_count.min_rows}"
        )
    if row_count > gates.row_count.max_rows:
        failures.append(
            f"row_count {row_count} above max_rows {gates.row_count.max_rows}"
        )

    return GateResult(
        passed=not failures,
        file_size_bytes=file_size,
        row_count=row_count,
        failures=failures,
    )
