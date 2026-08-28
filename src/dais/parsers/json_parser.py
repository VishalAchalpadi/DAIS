from __future__ import annotations

import json

import polars as pl

from dais.parsers.base import Parser, RawInput, register_parser
from dais.spec.models import ParserConfig


def _stringify(value):
    if value is None:
        return None
    if isinstance(value, (dict, list)):
        return json.dumps(value)
    if isinstance(value, bool):
        return "true" if value else "false"
    return str(value)


def _resolve_record_path(data, record_path: str | None):
    if not record_path:
        return data
    node = data
    for key in record_path.split("."):
        if not isinstance(node, dict) or key not in node:
            raise ValueError(f"record_path {record_path!r} not found in JSON document")
        node = node[key]
    return node


@register_parser("json")
class JsonParser(Parser):
    """Supports single JSON documents (object or array of objects, with an
    optional dotted `record_path` to the record array) and JSON-lines
    (`lines: true`, one JSON object per line)."""

    def parse(self, source: RawInput, config: ParserConfig) -> pl.DataFrame:
        raw_bytes = self._as_bytes(source)
        text = raw_bytes.decode("utf-8")

        if config.lines:
            records = [
                json.loads(line) for line in text.splitlines() if line.strip() != ""
            ]
        else:
            data = json.loads(text)
            records = _resolve_record_path(data, config.record_path)
            if isinstance(records, dict):
                records = [records]
            if not isinstance(records, list):
                raise ValueError("resolved JSON records must be a list of objects")

        rows = [
            {k: _stringify(v) for k, v in record.items()} for record in records
        ]

        if not rows:
            return pl.DataFrame()

        return pl.DataFrame(rows)
