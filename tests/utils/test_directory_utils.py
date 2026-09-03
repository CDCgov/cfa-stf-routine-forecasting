"""Tests for forecast output and model batch directory utilities."""

import datetime as dt

import pytest

from cfa.stf.routine.utils.directory_utils import (
    get_all_model_batch_dirs,
    get_model_batch_dir_name,
    parse_forecast_output_dir_name,
    parse_model_batch_dir_name,
)


def test_model_batch_dir_name_round_trip():
    name = get_model_batch_dir_name(
        disease="covid",
        n_training_days=150,
        exclude_last_n_days=4,
    )

    assert name == "covid_lookback-150_omit-4"
    assert parse_model_batch_dir_name(name) == {
        "disease": "covid",
        "n_training_days": 150,
        "exclude_last_n_days": 4,
    }


@pytest.mark.parametrize(
    "name",
    [
        "covid_r_2024-02-03_f_2021-04-01_t_2024-01-23",
        "covid_lookback-150_omit-4-extra",
        "covid_lookback-many_omit-4",
        "covid_lookback-150_omit-few",
    ],
)
def test_parse_model_batch_dir_name_rejects_invalid_format(name):
    with pytest.raises(ValueError, match="Invalid model batch directory"):
        parse_model_batch_dir_name(name)


def test_parse_model_batch_dir_name_rejects_unknown_disease():
    with pytest.raises(ValueError, match="Unknown disease"):
        parse_model_batch_dir_name("measles_lookback-150_omit-4")


def test_parse_forecast_output_dir_name():
    assert parse_forecast_output_dir_name("/tmp/2024-02-03_forecasts") == dt.date(
        2024, 2, 3
    )


@pytest.mark.parametrize(
    "name",
    ["2024-02-03", "2024-02-30_forecasts", "latest_forecasts"],
)
def test_parse_forecast_output_dir_name_rejects_invalid_name(name):
    with pytest.raises(ValueError):
        parse_forecast_output_dir_name(name)


def test_get_all_model_batch_dirs_filters_diseases_and_files(tmp_path):
    expected = {
        "covid_lookback-150_omit-1",
        "flu_lookback-90_omit-4",
    }
    for name in expected | {
        "rsv_lookback-60_omit-2",
        "covid_lookback-many_omit-1",
        "not-a-batch",
    }:
        (tmp_path / name).mkdir()
    (tmp_path / "covid_lookback-30_omit-0.txt").touch()

    assert set(get_all_model_batch_dirs(tmp_path, ["covid", "flu"])) == expected
