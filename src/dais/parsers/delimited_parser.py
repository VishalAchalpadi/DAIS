from __future__ import annotations

import io

import polars as pl

from dais.parsers.base import Parser, RawInput, register_parser
from dais.spec.models import ParserConfig


@register_parser("delimited")
class DelimitedParser(Parser):
    """Configurable-delimiter parser (pipe, tab, semicolon, ...). All
    columns come back as Utf8 - infer_schema_length=0 disables type
    inference so nothing is cast at parse time."""

    DEFAULT_DELIMITER = ","

    def parse(self, source: RawInput, config: ParserConfig) -> pl.DataFrame:
        raw_bytes = self._as_bytes(source)
        delimiter = config.delimiter or self.DEFAULT_DELIMITER
        quote_char = config.quote_char if config.quote_char is not None else '"'
        has_header = config.has_header if config.has_header is not None else True

        # Note: polars' CSV reader uses doubled-quote escaping and does not
        # expose a distinct escape_char; config.escape_char is accepted for
        # spec compatibility but is not yet honored by this implementation.
        return pl.read_csv(
            io.BytesIO(raw_bytes),
            separator=delimiter,
            quote_char=quote_char,
            has_header=has_header,
            infer_schema_length=0,
        )
