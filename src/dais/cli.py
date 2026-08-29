"""`dais run --spec specs/holdings_ingest.yaml --file path/to/file` -
runs a pipeline standalone, no API/Airflow required. Exits 0 on success,
1 on failure or quarantine, so it's safe to use in a shell script (see
the Control-M wrapper example in the README).
"""
from __future__ import annotations

import argparse
import sys

from dais.db import build_connector_for_spec
from dais.pipeline import run_pipeline
from dais.spec.loader import SpecLoadError, load_spec


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="dais")
    subparsers = parser.add_subparsers(dest="command", required=True)

    run_parser = subparsers.add_parser("run", help="run a pipeline from a spec")
    run_parser.add_argument("--spec", required=True, help="path to the pipeline spec YAML")
    run_parser.add_argument("--file", required=True, help="path to the source file to ingest")
    run_parser.add_argument(
        "--stop-after", choices=["raw", "stage", "gold"], default=None, help="override execution.stop_after"
    )

    args = parser.parse_args(argv)

    try:
        spec = load_spec(args.spec)
    except SpecLoadError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    connector, connection_params = build_connector_for_spec(spec)
    try:
        result = run_pipeline(
            spec,
            file_path=args.file,
            connector=connector,
            connection_params=connection_params,
            stop_after=args.stop_after,
        )
    finally:
        connector.close()

    print(f"run_id={result.run_id} status={result.status} layer_reached={result.layer_reached}")
    if result.error:
        print(f"error: {result.error}", file=sys.stderr)

    return 0 if result.status == "succeeded" else 1


if __name__ == "__main__":
    raise SystemExit(main())
