from __future__ import annotations

import polars as pl

from dais.parsers.base import Parser, RawInput, register_parser
from dais.spec.models import ParserConfig


@register_parser("fixed_width")
class FixedWidthParser(Parser):
    """Splits each line into fields by 1-indexed inclusive start/end
    byte-offsets defined in `parser.columns`. Pure slicing - no casting."""

    def parse(self, source: RawInput, config: ParserConfig) -> pl.DataFrame:
        if not config.columns:
            raise ValueError("fixed_width parser requires parser.columns")

        raw_bytes = self._as_bytes(source)
        text = raw_bytes.decode("utf-8")
        lines = [line for line in text.splitlines() if line.strip() != ""]

        rows: list[dict[str, str]] = []
        for line in lines:
            row = {
                col.name: line[col.start - 1 : col.end].strip()
                for col in config.columns
            }
            rows.append(row)

        column_names = [col.name for col in config.columns]
        if not rows:
            return pl.DataFrame({name: [] for name in column_names}, schema={n: pl.Utf8 for n in column_names})

        return pl.DataFrame(rows, schema={n: pl.Utf8 for n in column_names})
