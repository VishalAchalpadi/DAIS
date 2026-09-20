"""`dais run --spec specs/holdings_ingest.yaml --file path/to/file` -
runs a pipeline standalone, no API/Airflow required. Exits 0 on success,
1 on failure or quarantine, so it's safe to use in a shell script (see
the Control-M wrapper example in the README).
"""
from __future__ import annotations

import argparse
import json
import sys

from dais.db import build_connector_for_spec, build_s3_connector_for_spec
from dais.ingestion.file_discovery import FileDiscoveryError, resolve_files
from dais.lineage.column_lineage import build_lineage_graph
from dais.pipeline import run_pipeline
from dais.spec.loader import SpecLoadError, load_spec


def _resolve_file_paths(spec, args, s3) -> list[str] | None:
    """[args.file] if given (today's behavior, unchanged). Otherwise
    discovers file(s) from spec.source.location.multi_file - one path for
    "latest"/"earliest", several in order for "all". Returns None (having
    already printed an error) if discovery isn't usable."""
    if args.file is not None:
        return [args.file]

    location = spec.source.location
    if location.multi_file is None:
        print(
            "error: --file is required (spec has no source.location.multi_file configured)",
            file=sys.stderr,
        )
        return None
    try:
        resolved = resolve_files(location, s3 if location.kind == "s3" else None)
    except FileDiscoveryError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return None
    return [f.path for f in resolved]


def _run(args: argparse.Namespace) -> int:
    try:
        spec = load_spec(args.spec)
    except SpecLoadError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    connector, connection_params = build_connector_for_spec(spec)
    s3 = build_s3_connector_for_spec(spec)
    try:
        file_paths = _resolve_file_paths(spec, args, s3)
        if file_paths is None:
            return 1

        on_earlier_failure = None
        location = spec.source.location
        if location.multi_file is not None:
            on_earlier_failure = location.multi_file.on_earlier_failure

        exit_code = 0
        for file_path in file_paths:
            result = run_pipeline(
                spec,
                file_path=file_path,
                connector=connector,
                connection_params=connection_params,
                s3=s3,
                stop_after=args.stop_after,
            )
            print(
                f"run_id={result.run_id} status={result.status} "
                f"layer_reached={result.layer_reached} file={file_path}"
            )
            if result.error:
                print(f"error: {result.error}", file=sys.stderr)
            if result.status != "succeeded":
                exit_code = 1
            if (
                result.status == "failed"
                and len(file_paths) > 1
                and on_earlier_failure == "stop"
            ):
                print(
                    "stopping remaining files in this batch (on_earlier_failure: stop)",
                    file=sys.stderr,
                )
                break
        return exit_code
    finally:
        connector.close()


def _lineage(args: argparse.Namespace) -> int:
    try:
        spec = load_spec(args.spec)
    except SpecLoadError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    _, connection_params = build_connector_for_spec(spec)
    edges = build_lineage_graph(spec, **connection_params)

    payload = [
        {
            "source": {"layer": e.source.layer, "table": e.source.table, "column": e.source.column},
            "target": {"layer": e.target.layer, "table": e.target.table, "column": e.target.column},
            "transformation": e.transformation,
        }
        for e in edges
    ]
    output = json.dumps(payload, indent=2)
    if args.out:
        from pathlib import Path

        Path(args.out).write_text(output, encoding="utf-8")
        print(f"wrote {len(payload)} column-lineage edges to {args.out}")
    else:
        print(output)

    if args.sync_openmetadata:
        from dais.lineage.openmetadata_column_sync import sync_column_lineage

        sync_column_lineage(spec, connection_params)
        print(f"synced {len(payload)} column-lineage edges to OpenMetadata", file=sys.stderr)

    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="dais")
    subparsers = parser.add_subparsers(dest="command", required=True)

    run_parser = subparsers.add_parser("run", help="run a pipeline from a spec")
    run_parser.add_argument("--spec", required=True, help="path to the pipeline spec YAML")
    run_parser.add_argument(
        "--file",
        required=False,
        default=None,
        help=(
            "path to the source file to ingest. Omit to have the spec's "
            "source.location.multi_file config discover and select the "
            "file(s) to process instead."
        ),
    )
    run_parser.add_argument(
        "--stop-after", choices=["raw", "stage", "gold"], default=None, help="override execution.stop_after"
    )

    lineage_parser = subparsers.add_parser(
        "lineage", help="capture column-level lineage (raw->stage->gold) for a spec, as JSON"
    )
    lineage_parser.add_argument("--spec", required=True, help="path to the pipeline spec YAML")
    lineage_parser.add_argument("--out", default=None, help="write JSON to this path instead of stdout")
    lineage_parser.add_argument(
        "--sync-openmetadata",
        action="store_true",
        help=(
            "also push these column-level edges into OpenMetadata as detail on the "
            "raw->stage/stage->gold table lineage (requires the pipeline to have run "
            "at least once already, and OPENMETADATA_URL/OPENMETADATA_TOKEN set)"
        ),
    )

    args = parser.parse_args(argv)

    if args.command == "run":
        return _run(args)
    if args.command == "lineage":
        return _lineage(args)
    raise AssertionError(f"unhandled command: {args.command}")


if __name__ == "__main__":
    raise SystemExit(main())
