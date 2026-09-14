from __future__ import annotations

from xml.etree import ElementTree as ET

import polars as pl

from dais.parsers.base import Parser, RawInput, register_parser
from dais.spec.models import ParserConfig


def _flatten_record(element: ET.Element, prefix: str = "") -> dict[str, str | None]:
    """Child elements -> {tag: text}, recursing into any child that itself
    has children and joining tag names with '.' (e.g. an <Identifiers>
    wrapper around <ISIN> becomes "Identifiers.ISIN"). Attributes are
    included as "{tag}@attr" keys ("@attr" at the record's own top level).
    Recursing (rather than only reading immediate children) is required for
    real nested formats like FundsXML, where every value of interest sits
    inside a wrapper element rather than as a direct leaf."""
    row: dict[str, str | None] = {
        f"{prefix}@{k}": v for k, v in element.attrib.items()
    }
    for child in element:
        tag = f"{prefix}{child.tag}"
        if len(child) > 0:
            row.update(_flatten_record(child, prefix=f"{tag}."))
            continue
        text = child.text.strip() if child.text is not None else None
        row[tag] = text if text != "" else None
        for k, v in child.attrib.items():
            row[f"{tag}@{k}"] = v
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
