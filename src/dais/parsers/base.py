"""Common interface all file-format parsers implement.

A parser's only job is to split a raw file into a tabular structure of
string fields - no type casting, no validation. That happens later, at
the DQ stage (raw -> stage), driven by the spec's `quality.rules`.

Parsers are selected by name from the spec (`parser.type: fixed_width`),
so adding a new file format means adding a new module + registering it
here, never touching pipeline.py.
"""
from __future__ import annotations

from abc import ABC, abstractmethod
from pathlib import Path

import polars as pl

from dais.spec.models import ParserConfig

RawInput = bytes | str | Path


class Parser(ABC):
    """Abstract interface: parse(raw_bytes_or_path, config) -> DataFrame.

    All output columns are Utf8 (string) - every source field lands as
    plain text; casting is a stage-layer concern driven by `quality.rules`.
    """

    @abstractmethod
    def parse(self, source: RawInput, config: ParserConfig) -> pl.DataFrame:
        raise NotImplementedError

    @staticmethod
    def _as_bytes(source: RawInput) -> bytes:
        if isinstance(source, bytes):
            return source
        if isinstance(source, (str, Path)):
            return Path(source).read_bytes()
        raise TypeError(f"unsupported source type: {type(source)!r}")


_REGISTRY: dict[str, type[Parser]] = {}


def register_parser(name: str):
    """Class decorator registering a Parser implementation under `name`
    so it can be looked up by the spec's `parser.type` field."""

    def _decorator(cls: type[Parser]) -> type[Parser]:
        _REGISTRY[name] = cls
        return cls

    return _decorator


def get_parser(name: str) -> Parser:
    try:
        return _REGISTRY[name]()
    except KeyError as exc:
        available = ", ".join(sorted(_REGISTRY)) or "(none registered)"
        raise ValueError(
            f"no parser registered for type {name!r}; available: {available}"
        ) from exc
