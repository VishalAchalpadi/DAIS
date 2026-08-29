"""OpenLineage START/COMPLETE/FAIL event emission at each stage
transition. Every spec gets lineage for free from its `lineage.namespace`
+ `lineage.job_name` - no per-pipeline lineage code required.
"""
from __future__ import annotations

import os
from datetime import datetime, timezone

from openlineage.client import OpenLineageClient
from openlineage.client.run import Dataset, Job, Run, RunEvent, RunState
from openlineage.client.transport.console import ConsoleTransport
from openlineage.client.transport.transport import Transport

from dais.spec.models import PipelineSpec

_PRODUCER = "https://github.com/dais-framework/dais"


class LineageEmitter:
    def __init__(self, namespace: str, job_name: str, client: OpenLineageClient):
        self.namespace = namespace
        self.job_name = job_name
        self.client = client

    def _event(
        self,
        state: RunState,
        step: str,
        run_id: str,
        inputs: list[str] | None,
        outputs: list[str] | None,
    ) -> RunEvent:
        return RunEvent(
            eventType=state,
            eventTime=datetime.now(timezone.utc).isoformat(),
            run=Run(runId=run_id),
            job=Job(namespace=self.namespace, name=f"{self.job_name}.{step}"),
            producer=_PRODUCER,
            inputs=[Dataset(namespace=self.namespace, name=n) for n in (inputs or [])],
            outputs=[Dataset(namespace=self.namespace, name=n) for n in (outputs or [])],
        )

    def start(self, step: str, run_id: str, inputs: list[str] | None = None, outputs: list[str] | None = None) -> None:
        self.client.emit(self._event(RunState.START, step, run_id, inputs, outputs))

    def complete(self, step: str, run_id: str, inputs: list[str] | None = None, outputs: list[str] | None = None) -> None:
        self.client.emit(self._event(RunState.COMPLETE, step, run_id, inputs, outputs))

    def fail(self, step: str, run_id: str, inputs: list[str] | None = None, outputs: list[str] | None = None) -> None:
        self.client.emit(self._event(RunState.FAIL, step, run_id, inputs, outputs))


def build_client(transport: Transport | None = None) -> OpenLineageClient:
    if transport is not None:
        return OpenLineageClient(transport=transport)
    url = os.environ.get("OPENLINEAGE_URL")
    if url:
        return OpenLineageClient(url=url)
    return OpenLineageClient(transport=ConsoleTransport())


def build_emitter(spec: PipelineSpec, transport: Transport | None = None) -> LineageEmitter:
    return LineageEmitter(spec.lineage.namespace, spec.lineage.job_name, build_client(transport))
