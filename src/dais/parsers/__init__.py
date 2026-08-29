"""Importing this package registers all built-in parsers by name."""
from dais.parsers.base import Parser, get_parser, register_parser
from dais.parsers.csv_parser import CsvParser
from dais.parsers.delimited_parser import DelimitedParser
from dais.parsers.fixed_width_parser import FixedWidthParser, parse_fixed_width_chunked
from dais.parsers.json_parser import JsonParser
from dais.parsers.xml_parser import XmlParser

__all__ = [
    "Parser",
    "get_parser",
    "register_parser",
    "CsvParser",
    "DelimitedParser",
    "FixedWidthParser",
    "parse_fixed_width_chunked",
    "JsonParser",
    "XmlParser",
]
