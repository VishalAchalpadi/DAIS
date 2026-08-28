"""Loads a pipeline spec YAML file into a validated PipelineSpec."""
from __future__ import annotations

from pathlib import Path

import yaml
from pydantic import ValidationError

from dais.spec.models import PipelineSpec


class SpecLoadError(Exception):
    """Raised when a spec file cannot be read, parsed, or validated."""


def load_spec(path: str | Path) -> PipelineSpec:
    path = Path(path)
    if not path.is_file():
        raise SpecLoadError(f"spec file not found: {path}")

    try:
        raw_text = path.read_text(encoding="utf-8")
    except OSError as exc:
        raise SpecLoadError(f"could not read spec file {path}: {exc}") from exc

    try:
        data = yaml.safe_load(raw_text)
    except yaml.YAMLError as exc:
        raise SpecLoadError(f"invalid YAML in {path}: {exc}") from exc

    if not isinstance(data, dict):
        raise SpecLoadError(f"spec file {path} did not parse to a mapping/object")

    try:
        return PipelineSpec.model_validate(data)
    except ValidationError as exc:
        raise SpecLoadError(f"spec {path} failed validation:\n{exc}") from exc
