"""OpenLineage START/COMPLETE/FAIL event emission at each stage
transition. Every spec gets lineage for free from its `lineage.namespace`
+ `lineage.job_name` - no per-pipeline lineage code required.
"""
from __future__ import annotations

import os
from datetime import datetime, timezone

from openlineage.client import OpenLineageClient
from openlineage.client.run import Dataset, Job, Run, RunEvent, RunState
from openlineage.client.transport.console import ConsoleConfig, ConsoleTransport
from openlineage.client.transport.transport import Transport

from dais.spec.models import PipelineSpec

_PRODUCER = "https://github.com/dais-framework/dais"


class LineageEmitter:
    def __init__(
        self,
        namespace: str,
        job_name: str,
        client: OpenLineageClient,
        *,
        db_namespace: str | None = None,
        dbname: str | None = None,
    ):
        self.namespace = namespace
        self.job_name = job_name
        self.client = client
        # Set when the caller has real connection params (build_emitter,
        # given connection_params) - lets table datasets (raw/stage/gold
        # tables, shaped "schema.table") be identified the SAME way
        # dbt-ol identifies them (verified against its actual installed
        # source, not assumed): namespace "postgres://{host}:{port}", name
        # "{dbname}.{schema}.{table}". Without this, a gold_builds dbt
        # model's dbt-ol lineage and this pipeline's own raw/stage lineage
        # register the SAME physical stage table as two different dataset
        # identities, and an OpenLineage backend (Marquez, etc.) shows two
        # disconnected graphs instead of one connected one.
        self.db_namespace = db_namespace
        self.dbname = dbname

    def _dataset(self, identifier: str) -> Dataset:
        is_table = (
            self.db_namespace is not None
            and self.dbname is not None
            and "." in identifier
            and "/" not in identifier
            and "\\" not in identifier
        )
        if is_table:
            schema, table = identifier.split(".", 1)
            return Dataset(namespace=self.db_namespace, name=f"{self.dbname}.{schema}.{table}")
        # A source file (or anything else not shaped "schema.table") has
        # no external system's dataset identity to match - keep it under
        # this job's own logical namespace, same as always.
        return Dataset(namespace=self.namespace, name=identifier)

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
            inputs=[self._dataset(n) for n in (inputs or [])],
            outputs=[self._dataset(n) for n in (outputs or [])],
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
    return OpenLineageClient(transport=ConsoleTransport(ConsoleConfig()))


def build_emitter(
    spec: PipelineSpec, transport: Transport | None = None, *, connection_params: dict | None = None
) -> LineageEmitter:
    db_namespace = None
    dbname = None
    if connection_params and spec.database.platform == "postgres":
        db_namespace = f"postgres://{connection_params['host']}:{connection_params['port']}"
        dbname = connection_params["dbname"]
    return LineageEmitter(
        spec.lineage.namespace,
        spec.lineage.job_name,
        build_client(transport),
        db_namespace=db_namespace,
        dbname=dbname,
    )
