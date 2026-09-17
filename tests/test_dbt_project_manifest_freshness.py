"""Regression tests for dbt_project.py's manifest-freshness check.

dbt_project.py's prepare_if_dev()/prepare() call used to run an
unconditional `dbt parse` on every process that imported the module -
including a fresh subprocess Dagster spawns per run - at a measured cost
of ~45s per parse for work that was almost always a no-op (nothing in the
dbt project had changed). _manifest_is_fresh()/_dbt_project_changed_since()
gate that reparse on real staleness instead. These tests exercise the
staleness detection directly rather than timing an actual `dbt parse`
(slow, and the timing itself isn't what needs covering).
"""
import os
import time

from dais.orchestration.dagster.dbt_project import (
    DBT_PROJECT,
    DBT_PROJECT_DIR,
    _dbt_project_changed_since,
    _manifest_is_fresh,
)


def test_manifest_is_fresh_when_up_to_date():
    # The real project's manifest is kept up to date by this same module's
    # own import-time logic (already run once just importing this test's
    # dependencies) - so immediately after import, it must report fresh.
    assert _manifest_is_fresh() is True


def test_changed_since_detects_a_newer_source_file():
    manifest_mtime = DBT_PROJECT.manifest_path.stat().st_mtime
    a_model = next(DBT_PROJECT_DIR.glob("models/*.sql"))
    original_mtime = a_model.stat().st_mtime
    try:
        future = time.time() + 3600
        os.utime(a_model, (future, future))
        assert _dbt_project_changed_since(manifest_mtime) is True
    finally:
        os.utime(a_model, (original_mtime, original_mtime))


def test_changed_since_is_false_when_nothing_is_newer():
    # A reference time far in the future can't be beaten by any real file.
    assert _dbt_project_changed_since(time.time() + 3600) is False


def test_target_directory_is_excluded_from_staleness_checks():
    # target/ holds the manifest's own generated output (run_results.json,
    # compiled SQL, etc.), rewritten by every real dbt invocation - if it
    # counted as a "source" file, the manifest would look stale again
    # immediately after being regenerated, making the freshness check
    # pointless. A brand-new file inside target/ must NOT trigger staleness.
    manifest_mtime = DBT_PROJECT.manifest_path.stat().st_mtime
    scratch = DBT_PROJECT_DIR / "target" / "_freshness_test_scratch.yml"
    scratch.write_text("scratch: true\n", encoding="utf-8")
    try:
        assert _dbt_project_changed_since(manifest_mtime) is False
    finally:
        scratch.unlink()
