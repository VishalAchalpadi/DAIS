"""GoldBuildRunRegistry - pure unit tests, no DB/dbt involved. The
concurrent-run guard (start() returns None while a build is already
running) can't be exercised through the HTTP API in test_gold_build_api.py
because TestClient's BackgroundTasks run synchronously before the response
returns, so there's no window where a real run is "in flight" from the
test's perspective - tested directly against the registry instead."""
from dais.api.gold_build_runs import GoldBuildRunRegistry


def test_start_returns_a_run_id_and_tracks_it_as_running():
    registry = GoldBuildRunRegistry()
    run_id = registry.start("fx_rates_gold")
    assert run_id is not None
    record = registry.get(run_id)
    assert record.gold_build_name == "fx_rates_gold"
    assert record.status == "running"


def test_start_refuses_a_second_concurrent_run_of_the_same_build():
    registry = GoldBuildRunRegistry()
    registry.start("fx_rates_gold")
    assert registry.start("fx_rates_gold") is None


def test_a_different_build_can_start_concurrently():
    registry = GoldBuildRunRegistry()
    registry.start("fx_rates_gold")
    assert registry.start("regional_sales_gold") is not None


def test_finish_records_terminal_state_and_frees_the_build_to_run_again():
    registry = GoldBuildRunRegistry()
    run_id = registry.start("fx_rates_gold")
    registry.finish(run_id, "succeeded")
    assert registry.get(run_id).status == "succeeded"
    assert registry.start("fx_rates_gold") is not None  # no longer "running"


def test_finish_records_an_error_message():
    registry = GoldBuildRunRegistry()
    run_id = registry.start("fx_rates_gold")
    registry.finish(run_id, "failed", error="dbt exited 1")
    record = registry.get(run_id)
    assert record.status == "failed"
    assert record.error == "dbt exited 1"


def test_unknown_run_id_returns_none():
    registry = GoldBuildRunRegistry()
    assert registry.get("not-a-real-run-id") is None
