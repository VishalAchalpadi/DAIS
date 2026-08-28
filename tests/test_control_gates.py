import pytest

from dais.quality.control_gates import (
    compute_approx_row_count,
    compute_fixed_width_row_count,
    run_control_gates,
)
from dais.spec.models import ControlGatesConfig, FixedWidthColumn, ParserConfig


def _gates(min_bytes=1, max_bytes=1000, min_rows=1, max_rows=100):
    return ControlGatesConfig(
        file_size={"min_bytes": min_bytes, "max_bytes": max_bytes},
        row_count={"min_rows": min_rows, "max_rows": max_rows},
    )


def _fw_columns():
    return [
        FixedWidthColumn(name="account_id", start=1, end=5),
        FixedWidthColumn(name="qty", start=6, end=10),
    ]


# ---------------------------------------------------------------------------
# fixed-width exact row count
# ---------------------------------------------------------------------------

def test_fixed_width_row_count_unix_newlines():
    raw = b"ACC01 1000\nACC02 2000\nACC03 3000\n"  # 10-byte record + \n = 11
    assert compute_fixed_width_row_count(raw, _fw_columns()) == 3


def test_fixed_width_row_count_windows_newlines():
    raw = b"ACC01 1000\r\nACC02 2000\r\n"  # 10-byte record + \r\n = 12
    assert compute_fixed_width_row_count(raw, _fw_columns()) == 2


def test_fixed_width_row_count_empty_file():
    assert compute_fixed_width_row_count(b"", _fw_columns()) == 0


# ---------------------------------------------------------------------------
# approximate row count (csv/json/xml)
# ---------------------------------------------------------------------------

def test_approx_row_count_counts_lines():
    raw = b"a,b\n1,2\n3,4\n"
    assert compute_approx_row_count(raw, has_header=False) == 3


def test_approx_row_count_subtracts_header():
    raw = b"a,b\n1,2\n3,4\n"
    assert compute_approx_row_count(raw, has_header=True) == 2


def test_approx_row_count_unterminated_last_line():
    raw = b"a,b\n1,2"
    assert compute_approx_row_count(raw, has_header=False) == 2


# ---------------------------------------------------------------------------
# run_control_gates - pass/fail on each bound
# ---------------------------------------------------------------------------

def test_gate_passes_within_bounds():
    raw = b"ACC01 1000\nACC02 2000\n"
    parser = ParserConfig(type="fixed_width", columns=_fw_columns())
    result = run_control_gates(raw, _gates(min_bytes=1, max_bytes=1000, min_rows=1, max_rows=10), parser)
    assert result.passed
    assert result.failures == []
    assert result.row_count == 2


def test_gate_fails_file_too_small():
    raw = b"ACC01 1000\n"
    parser = ParserConfig(type="fixed_width", columns=_fw_columns())
    result = run_control_gates(raw, _gates(min_bytes=100, max_bytes=1000, min_rows=0, max_rows=10), parser)
    assert not result.passed
    assert any("below min_bytes" in f for f in result.failures)


def test_gate_fails_file_too_large():
    raw = b"ACC01 1000\n" * 5
    parser = ParserConfig(type="fixed_width", columns=_fw_columns())
    result = run_control_gates(raw, _gates(min_bytes=1, max_bytes=10, min_rows=0, max_rows=100), parser)
    assert not result.passed
    assert any("above max_bytes" in f for f in result.failures)


def test_gate_fails_row_count_too_low():
    raw = b"ACC01 1000\n"
    parser = ParserConfig(type="fixed_width", columns=_fw_columns())
    result = run_control_gates(raw, _gates(min_bytes=1, max_bytes=1000, min_rows=5, max_rows=100), parser)
    assert not result.passed
    assert any("below min_rows" in f for f in result.failures)


def test_gate_fails_row_count_too_high():
    raw = (b"ACC01 1000\n") * 10
    parser = ParserConfig(type="fixed_width", columns=_fw_columns())
    result = run_control_gates(raw, _gates(min_bytes=1, max_bytes=1000, min_rows=0, max_rows=3), parser)
    assert not result.passed
    assert any("above max_rows" in f for f in result.failures)


def test_gate_reports_multiple_failures_at_once():
    raw = b"ACC01 1000\n"
    parser = ParserConfig(type="fixed_width", columns=_fw_columns())
    result = run_control_gates(
        raw, _gates(min_bytes=1000, max_bytes=2000, min_rows=5, max_rows=100), parser
    )
    assert not result.passed
    assert len(result.failures) == 2
