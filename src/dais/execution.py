"""`execution.stop_after` handling: which layers a run should execute,
and whether the run should stop after a given layer completes. The full
orchestrator (pipeline.py) that calls this is built in Phase 7 - this
module is the standalone, independently-testable piece of that logic.

Stopping early is a normal, complete run, not a partial/failed one.
"""
from __future__ import annotations

_LAYER_ORDER = ["raw", "stage", "gold"]


def layers_to_run(stop_after: str) -> list[str]:
    if stop_after not in _LAYER_ORDER:
        raise ValueError(f"unknown stop_after layer: {stop_after!r}; must be one of {_LAYER_ORDER}")
    return _LAYER_ORDER[: _LAYER_ORDER.index(stop_after) + 1]


def is_stop_layer(layer: str, stop_after: str) -> bool:
    return layer == stop_after
