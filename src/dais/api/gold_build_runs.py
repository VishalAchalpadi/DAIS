"""Run registry for gold_builds/*.yaml runs triggered via the API - a
sibling to api/runs.py's RunRegistry, not a reuse of it. A gold build has
no input file (it reads its dependencies' stage tables) and no
raw/stage/gold ladder of its own (see docs/running-pipelines.md, "Gold
layer: two mechanisms"), so neither of RunRegistry's two organizing ideas
(checksum dedup, layer_reached) applies here - this tracks only what a
gold build run actually has: a run_id, which build it was, and whether it
's currently in flight (so two overlapping runs against the same target
don't stomp on each other; wasted work, not unsafe, but no reason to allow
it).
"""
from __future__ import annotations

import threading
import uuid
from dataclasses import dataclass, field


@dataclass
class GoldBuildRunRecord:
    run_id: str
    gold_build_name: str
    status: str  # "running" | "succeeded" | "failed"
    error: str | None = None


class GoldBuildRunRegistry:
    def __init__(self):
        self._lock = threading.Lock()
        self._by_run_id: dict[str, GoldBuildRunRecord] = {}
        self._running: set[str] = set()

    def start(self, gold_build_name: str) -> str | None:
        """Returns a new run_id, or None if this build is already running."""
        with self._lock:
            if gold_build_name in self._running:
                return None
            run_id = str(uuid.uuid4())
            self._by_run_id[run_id] = GoldBuildRunRecord(
                run_id=run_id, gold_build_name=gold_build_name, status="running"
            )
            self._running.add(gold_build_name)
            return run_id

    def get(self, run_id: str) -> GoldBuildRunRecord | None:
        with self._lock:
            return self._by_run_id.get(run_id)

    def finish(self, run_id: str, status: str, error: str | None = None) -> None:
        with self._lock:
            record = self._by_run_id[run_id]
            record.status = status
            record.error = error
            self._running.discard(record.gold_build_name)
