from __future__ import annotations

from pathlib import Path

import yaml
from pydantic import ValidationError

from dais.business_process.models import BusinessProcessGroup


class BusinessProcessLoadError(Exception):
    pass


def load_business_process(path: str | Path) -> BusinessProcessGroup:
    path = Path(path)
    if not path.is_file():
        raise BusinessProcessLoadError(f"business process file not found: {path}")

    data = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(data, dict):
        raise BusinessProcessLoadError(f"{path} did not parse to a mapping/object")

    try:
        return BusinessProcessGroup.model_validate(data)
    except ValidationError as exc:
        raise BusinessProcessLoadError(f"business process {path} failed validation:\n{exc}") from exc
