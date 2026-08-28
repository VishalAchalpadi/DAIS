from __future__ import annotations

import polars as pl

from dais.parsers.base import Parser, RawInput, register_parser
from dais.parsers.delimited_parser import DelimitedParser
from dais.spec.models import ParserConfig


@register_parser("csv")
class CsvParser(Parser):
    """Standard header-aware CSV. Thin wrapper over DelimitedParser with
    delimiter fixed to ','."""

    def parse(self, source: RawInput, config: ParserConfig) -> pl.DataFrame:
        csv_config = config.model_copy(update={"delimiter": ","})
        return DelimitedParser().parse(source, csv_config)
