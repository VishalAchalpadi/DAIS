import pytest

from dais.execution import is_stop_layer, layers_to_run


def test_stop_after_raw_runs_only_raw():
    assert layers_to_run("raw") == ["raw"]


def test_stop_after_stage_runs_raw_and_stage():
    assert layers_to_run("stage") == ["raw", "stage"]


def test_stop_after_gold_runs_all_layers():
    assert layers_to_run("gold") == ["raw", "stage", "gold"]


def test_unknown_stop_after_raises():
    with pytest.raises(ValueError, match="unknown stop_after"):
        layers_to_run("bronze")


def test_is_stop_layer_true_at_configured_layer():
    assert is_stop_layer("stage", stop_after="stage") is True


def test_is_stop_layer_false_before_configured_layer():
    assert is_stop_layer("raw", stop_after="stage") is False


def test_is_stop_layer_false_after_configured_layer():
    assert is_stop_layer("gold", stop_after="stage") is False
