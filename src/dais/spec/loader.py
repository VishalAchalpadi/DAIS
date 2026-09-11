"""Loads a pipeline spec YAML file into a validated PipelineSpec, or a
gold_builds/*.yaml file into a validated GoldBuildSpec (Phase 9a)."""
from __future__ import annotations

from pathlib import Path

import yaml
from pydantic import BaseModel, ValidationError

from dais.spec.models import GoldBuildSpec, PipelineSpec


class SpecLoadError(Exception):
    """Raised when a spec file cannot be read, parsed, or validated."""


def _read_yaml_mapping(path: Path) -> dict:
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
    return data


def _validate(model: type[BaseModel], data: dict, path: Path):
    try:
        return model.model_validate(data)
    except ValidationError as exc:
        raise SpecLoadError(f"spec {path} failed validation:\n{exc}") from exc


def load_spec(path: str | Path) -> PipelineSpec:
    path = Path(path)
    return _validate(PipelineSpec, _read_yaml_mapping(path), path)


def load_gold_build_spec(path: str | Path) -> GoldBuildSpec:
    path = Path(path)
    return _validate(GoldBuildSpec, _read_yaml_mapping(path), path)
