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
from dais.lineage.column_lineage import build_lineage_graph
from dais.pipeline import run_pipeline
from dais.spec.loader import SpecLoadError, load_spec


def _run(args: argparse.Namespace) -> int:
    try:
        spec = load_spec(args.spec)
    except SpecLoadError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    connector, connection_params = build_connector_for_spec(spec)
    s3 = build_s3_connector_for_spec(spec)
    try:
        result = run_pipeline(
            spec,
            file_path=args.file,
            connector=connector,
            connection_params=connection_params,
            s3=s3,
            stop_after=args.stop_after,
        )
    finally:
        connector.close()

    print(f"run_id={result.run_id} status={result.status} layer_reached={result.layer_reached}")
    if result.error:
        print(f"error: {result.error}", file=sys.stderr)

    return 0 if result.status == "succeeded" else 1


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
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="dais")
    subparsers = parser.add_subparsers(dest="command", required=True)

    run_parser = subparsers.add_parser("run", help="run a pipeline from a spec")
    run_parser.add_argument("--spec", required=True, help="path to the pipeline spec YAML")
    run_parser.add_argument("--file", required=True, help="path to the source file to ingest")
    run_parser.add_argument(
        "--stop-after", choices=["raw", "stage", "gold"], default=None, help="override execution.stop_after"
    )

    lineage_parser = subparsers.add_parser(
        "lineage", help="capture column-level lineage (raw->stage->gold) for a spec, as JSON"
    )
    lineage_parser.add_argument("--spec", required=True, help="path to the pipeline spec YAML")
    lineage_parser.add_argument("--out", default=None, help="write JSON to this path instead of stdout")

    args = parser.parse_args(argv)

    if args.command == "run":
        return _run(args)
    if args.command == "lineage":
        return _lineage(args)
    raise AssertionError(f"unhandled command: {args.command}")


if __name__ == "__main__":
    raise SystemExit(main())
