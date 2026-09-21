"""OpenLineage START/COMPLETE/FAIL event emission at each stage
transition. Every spec gets lineage for free from its `lineage.namespace`
+ `lineage.job_name` - no per-pipeline lineage code required.
"""
from __future__ import annotations

import os
from datetime import datetime, timezone

from openlineage.client import OpenLineageClient
from openlineage.client.facet_v2 import column_lineage_dataset, schema_dataset
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

    def _dataset(self, identifier: str, facets: dict | None = None) -> Dataset:
        is_table = (
            self.db_namespace is not None
            and self.dbname is not None
            and "." in identifier
            and "/" not in identifier
            and "\\" not in identifier
        )
        if is_table:
            schema, table = identifier.split(".", 1)
            return Dataset(namespace=self.db_namespace, name=f"{self.dbname}.{schema}.{table}", facets=facets or {})
        # A source file (or anything else not shaped "schema.table") has
        # no external system's dataset identity to match - keep it under
        # this job's own logical namespace, same as always.
        return Dataset(namespace=self.namespace, name=identifier, facets=facets or {})

    def _facets(
        self,
        identifier: str,
        schemas: dict[str, list[tuple[str, str]]] | None,
        column_lineage: dict[str, list[tuple[str, str, str, str | None]]] | None,
    ) -> dict:
        """Standard OpenLineage `schema` / `columnLineage` dataset facets for
        one output dataset - what lets a backend (openmetadata_forwarder.py)
        show columns and column-level lineage without any DB access or extra
        step. schemas: dataset -> [(column, type)]. column_lineage: output
        dataset -> [(output column, input dataset, input column, description)]."""
        facets: dict = {}
        if schemas and identifier in schemas:
            facets["schema"] = schema_dataset.SchemaDatasetFacet(
                fields=[schema_dataset.SchemaDatasetFacetFields(name=n, type=t) for n, t in schemas[identifier]]
            )
        if column_lineage and identifier in column_lineage:
            by_output: dict[str, list[column_lineage_dataset.InputField]] = {}
            descriptions: dict[str, str | None] = {}
            for out_col, in_identifier, in_col, description in column_lineage[identifier]:
                source = self._dataset(in_identifier)
                by_output.setdefault(out_col, []).append(
                    column_lineage_dataset.InputField(namespace=source.namespace, name=source.name, field=in_col)
                )
                descriptions.setdefault(out_col, description)
            facets["columnLineage"] = column_lineage_dataset.ColumnLineageDatasetFacet(
                fields={
                    out_col: column_lineage_dataset.Fields(
                        inputFields=inputs,
                        transformationDescription=descriptions.get(out_col),
                        transformationType="TRANSFORMATION",
                    )
                    for out_col, inputs in by_output.items()
                }
            )
        return facets

    def _event(
        self,
        state: RunState,
        step: str,
        run_id: str,
        inputs: list[str] | None,
        outputs: list[str] | None,
        schemas: dict[str, list[tuple[str, str]]] | None = None,
        column_lineage: dict[str, list[tuple[str, str, str, str | None]]] | None = None,
    ) -> RunEvent:
        return RunEvent(
            eventType=state,
            eventTime=datetime.now(timezone.utc).isoformat(),
            run=Run(runId=run_id),
            job=Job(namespace=self.namespace, name=f"{self.job_name}.{step}"),
            producer=_PRODUCER,
            inputs=[self._dataset(n) for n in (inputs or [])],
            outputs=[self._dataset(n, self._facets(n, schemas, column_lineage)) for n in (outputs or [])],
        )

    def start(self, step: str, run_id: str, inputs: list[str] | None = None, outputs: list[str] | None = None) -> None:
        self.client.emit(self._event(RunState.START, step, run_id, inputs, outputs))

    def complete(
        self,
        step: str,
        run_id: str,
        inputs: list[str] | None = None,
        outputs: list[str] | None = None,
        *,
        schemas: dict[str, list[tuple[str, str]]] | None = None,
        column_lineage: dict[str, list[tuple[str, str, str, str | None]]] | None = None,
    ) -> None:
        self.client.emit(self._event(RunState.COMPLETE, step, run_id, inputs, outputs, schemas, column_lineage))

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
