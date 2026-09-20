"""A thin client for the EXISTING DAIS HTTP API (dais.api.app), used by
Dagster's raw/stage assets so they call the same trigger/poll surface
Control-M's shell wrapper already uses - never a separate execution path
for raw/stage. Dagster only ever sees run_id/status/layer_reached; the
actual ingestion logic (parsing, control gates, quality checks,
quarantine) all still lives where it always has, in dais.pipeline.
"""
from __future__ import annotations

import time

import requests
from dagster import ConfigurableResource


class DaisApiTimeoutError(Exception):
    pass


class DaisApiRunFailedError(Exception):
    def __init__(self, run_id: str, status: str, error: str | None, failure_reasons: list[str]):
        self.run_id = run_id
        self.status = status
        self.error = error
        self.failure_reasons = failure_reasons
        super().__init__(f"run {run_id} ended in status={status!r}: {error or failure_reasons}")


class DaisApiResource(ConfigurableResource):
    """Config maps 1:1 onto the API's own contract (dais.api.models) -
    no DAIS-specific business logic lives here, just HTTP plumbing."""

    base_url: str = "http://localhost:8000"
    # Pass api_key=EnvVar("DAIS_API_KEY") when constructing this resource in
    # definitions.py, rather than defaulting it here, so Dagster's config
    # system resolves it at launch time (and can redact it in the UI) the
    # same way it would for any other secret-shaped resource config.
    api_key: str = ""
    poll_interval_seconds: float = 2.0
    timeout_seconds: float = 1800.0

    def _headers(self) -> dict[str, str]:
        return {"X-API-Key": self.api_key} if self.api_key else {}

    def trigger_run(self, spec_name: str, file_path: str, stop_after: str | None = None) -> str:
        return self.trigger(spec_name, file_path, stop_after)["run_id"]

    def trigger(self, spec_name: str, file_path: str | None = None, stop_after: str | None = None) -> dict:
        """Raw trigger response - {"run_id": ...} (RunResponse) or
        {"run_ids": [...]} (RunBatchResponse, when file_path is omitted
        and the spec's source.location.multi_file resolves to more than
        one file - see dais.ingestion.file_discovery). trigger_run/
        run_and_wait assume the single-run shape; run_and_wait_many
        handles either."""
        resp = requests.post(
            f"{self.base_url}/pipelines/{spec_name}/run",
            json={"file_path": file_path, "stop_after": stop_after},
            headers=self._headers(),
            timeout=30,
        )
        resp.raise_for_status()
        return resp.json()

    def get_status(self, run_id: str) -> dict:
        resp = requests.get(
            f"{self.base_url}/pipelines/runs/{run_id}/status",
            headers=self._headers(),
            timeout=30,
        )
        resp.raise_for_status()
        return resp.json()

    def wait_for_completion(self, run_id: str) -> dict:
        """Polls status until it leaves "running" - the API itself is the
        single source of truth (it in turn reflects control.process_monitor
        via dais.pipeline), so this never re-derives completion from
        anything else (e.g. checking the target table directly)."""
        deadline = time.monotonic() + self.timeout_seconds
        while True:
            status_payload = self.get_status(run_id)
            if status_payload["status"] != "running":
                return status_payload
            if time.monotonic() >= deadline:
                raise DaisApiTimeoutError(
                    f"run {run_id} still 'running' after {self.timeout_seconds}s"
                )
            time.sleep(self.poll_interval_seconds)

    def run_and_wait(self, spec_name: str, file_path: str, stop_after: str | None = None) -> dict:
        run_id = self.trigger_run(spec_name, file_path, stop_after)
        result = self.wait_for_completion(run_id)
        if result["status"] != "succeeded":
            raise DaisApiRunFailedError(
                run_id, result["status"], result.get("error"), result.get("failure_reasons", [])
            )
        return result

    def run_and_wait_many(self, spec_name: str, stop_after: str | None = None) -> list[dict]:
        """Discovery path: file_path omitted, server resolves the spec's
        source.location.multi_file itself (one file for "latest"/
        "earliest", N files in order for "all"). Waits for every
        resulting run in turn (mirroring the server's own sequential
        execution) and raises on the first one that didn't succeed -
        including "skipped" (an earlier file in the batch failed under
        on_earlier_failure: "stop"), which is deliberately not treated as
        success here either."""
        response = self.trigger(spec_name, file_path=None, stop_after=stop_after)
        run_ids = response["run_ids"] if "run_ids" in response else [response["run_id"]]
        results = []
        for run_id in run_ids:
            result = self.wait_for_completion(run_id)
            results.append(result)
            if result["status"] != "succeeded":
                raise DaisApiRunFailedError(
                    run_id, result["status"], result.get("error"), result.get("failure_reasons", [])
                )
        return results
