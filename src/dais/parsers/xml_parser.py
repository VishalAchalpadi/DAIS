from __future__ import annotations

from xml.etree import ElementTree as ET

import polars as pl

from dais.parsers.base import Parser, RawInput, register_parser
from dais.spec.models import ParserConfig


def _flatten_record(element: ET.Element) -> dict[str, str | None]:
    """Immediate child elements -> {tag: text}. Attributes on the record
    element itself are included as "@attr" keys."""
    row: dict[str, str | None] = {
        f"@{k}": v for k, v in element.attrib.items()
    }
    for child in element:
        text = child.text.strip() if child.text is not None else None
        row[child.tag] = text if text != "" else None
    return row


@register_parser("xml")
class XmlParser(Parser):
    """Selects record elements via a configurable XPath (`record_xpath`)
    and flattens each one's immediate children into a tabular row."""

    def parse(self, source: RawInput, config: ParserConfig) -> pl.DataFrame:
        if not config.record_xpath:
            raise ValueError("xml parser requires parser.record_xpath")

        raw_bytes = self._as_bytes(source)
        root = ET.fromstring(raw_bytes)

        elements = root.findall(config.record_xpath)
        rows = [_flatten_record(el) for el in elements]

        if not rows:
            return pl.DataFrame()

        return pl.DataFrame(rows)
