"""A business process (e.g. "morning holdings recon") spans several
pipeline specs, so it's defined in its own file, not derived from any
one spec's `business_process:` tag - that tag is self-documentation
only; this file is the single source of truth for membership and SLA.
"""
from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, ConfigDict, Field


class StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid")


class BusinessProcessMember(StrictModel):
    pipeline_name: str
    success_layer: Literal["raw", "stage", "gold"] = "gold"


class SlaConfig(StrictModel):
    complete_by: str = Field(pattern=r"^\d{2}:\d{2}$")  # "HH:MM", 24h
    timezone: str  # IANA zone name, e.g. "America/New_York"


class BusinessProcessGroup(StrictModel):
    process_name: str
    members: list[BusinessProcessMember] = Field(min_length=1)
    sla: SlaConfig
