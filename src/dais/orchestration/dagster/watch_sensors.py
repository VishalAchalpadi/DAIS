"""Event-driven file watching as Dagster sensors - one per pipeline whose
spec sets `source.location.watch` (see spec/models.py's WatchConfig).

Each tick lists the watched location via ingestion.file_discovery (the
same side-effect-free listing/pattern-matching the on-demand multi-file
trigger uses) and yields one RunRequest per matching file, targeting that
pipeline's existing `{pipeline_name}_job` - so a sensor-launched run flows
through the same ingest asset, DAIS HTTP API, checksum dedup, quarantine
and lineage as a manual or API-triggered one. Dagster itself dedupes
RunRequests by run_key across ticks/restarts, so re-yielding a file that's
still sitting in the directory every tick is harmless.

Also fires the spec's missed_arrival_alert (once per day) if no file
matching the pattern has arrived by watch.expected_by. The cursor only
tracks the two things Dagster doesn't: the last date a file arrived, and
the last date an alert already fired.
"""
from __future__ import annotations

import json
from datetime import datetime
from zoneinfo import ZoneInfo

from dagster import DefaultSensorStatus, RunRequest, SensorDefinition, SensorEvaluationContext, SensorResult, sensor

from dais.db import build_s3_connector_for_spec
from dais.ingestion.file_discovery import FileDiscoveryError, NoMatchingFilesError, discover_files
from dais.quality.dq_alerts import get_alerter
from dais.spec.models import PipelineSpec


def _local_date(sort_key: datetime, tz: ZoneInfo) -> str:
    """A file's date in the watch's timezone. arrival_time sort keys are
    tz-aware (converted); filename_timestamp ones are naive dates parsed
    from the name, which are ALREADY the date the producer meant - treating
    them as server-local would shift the day on a pod whose timezone differs
    from expected_timezone and raise false missed-arrival alerts."""
    if sort_key.tzinfo is None:
        return sort_key.date().isoformat()
    return sort_key.astimezone(tz).date().isoformat()


def build_watch_sensor(pipeline_name: str, spec: PipelineSpec, job) -> SensorDefinition:
    location = spec.source.location
    watch = location.watch
    tz = ZoneInfo(watch.expected_timezone)

    @sensor(
        name=f"{pipeline_name}_watch_sensor",
        job=job,
        minimum_interval_seconds=watch.poll_interval_seconds,
        # Dagster sensors default to STOPPED; watch.enabled in the spec is
        # the opt-in, so don't also require a manual toggle in the UI.
        default_status=DefaultSensorStatus.RUNNING,
        description=f"Watches {location.path} for files matching {location.file_pattern!r} and runs {pipeline_name} on arrival.",
    )
    def _watch_sensor(context: SensorEvaluationContext) -> SensorResult:
        cursor = json.loads(context.cursor) if context.cursor else {}

        s3 = build_s3_connector_for_spec(spec) if location.kind == "s3" else None
        try:
            # Oldest first (per multi_file.order_by), so runs are submitted in
            # arrival order - Dagster queues by submission time.
            discovered = sorted(discover_files(location, s3, strict=False), key=lambda f: f.sort_key)
        except FileDiscoveryError as exc:
            # Zero matches is the normal steady state for a watcher, not an
            # error - and an unimplemented kind (sftp) shouldn't crash the
            # tick either, just watch nothing.
            if isinstance(exc, NoMatchingFilesError) and exc.unmatched:
                # Files ARE there but none match - almost always a wrong
                # extension or name. Say so, or it looks like a dead sensor.
                context.log.warning(f"{pipeline_name}: ignoring files that do not match the expected name: {exc}")
            else:
                context.log.debug(f"{pipeline_name}: nothing to dispatch this tick ({exc})")
            discovered = []

        now = datetime.now(tz)
        today = now.date().isoformat()

        # "Arrived today" means a discovered file's OWN date (per
        # multi_file.order_by) is today - not merely that some file is
        # present, or a stale leftover from a prior day would mask a
        # genuinely missed arrival.
        if any(_local_date(f.sort_key, tz) == today for f in discovered):
            cursor["last_arrival_date"] = today
            cursor["alerted_date"] = None

        run_requests = [
            RunRequest(
                run_key=f"{pipeline_name}:{f.path}:{f.sort_key.isoformat()}",
                run_config={
                    "ops": {
                        f"{pipeline_name}__stage": {
                            "config": {"file_path": f.path, "stop_after": spec.execution.stop_after}
                        }
                    }
                },
            )
            for f in discovered
        ]

        if watch.expected_by:
            hour, minute = map(int, watch.expected_by.split(":"))
            deadline = now.replace(hour=hour, minute=minute, second=0, microsecond=0)
            if now >= deadline and cursor.get("last_arrival_date") != today and cursor.get("alerted_date") != today:
                alert = watch.missed_arrival_alert
                get_alerter(alert.channel).send(
                    alert.destination,
                    f"{pipeline_name}: no file matching {location.file_pattern!r} has arrived "
                    f"by {watch.expected_by} {watch.expected_timezone}",
                    {"pipeline_name": pipeline_name, "expected_by": watch.expected_by},
                )
                cursor["alerted_date"] = today

        return SensorResult(run_requests=run_requests, cursor=json.dumps(cursor))

    return _watch_sensor
