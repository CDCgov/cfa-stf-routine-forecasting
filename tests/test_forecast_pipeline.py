import datetime as dt
import logging
from dataclasses import replace

import pytest

from cfa.stf.routine.forecast_pipeline import ForecastPipeline
from cfa.stf.routine.forecast_run import ForecastRun
from cfa.stf.routine.forecast_window import ForecastWindow
from tests.factories import make_test_forecast_run, make_test_surveillance_inputs


class TestPipeline(ForecastPipeline):
    __test__ = False

    def __init__(
        self,
        *,
        events=None,
        ed_visit_input_resolution="daily",
        minimum_exclude_last_n_days=0,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.events = events if events is not None else []
        self._ed_visit_input_resolution = ed_visit_input_resolution
        self._minimum_exclude_last_n_days = minimum_exclude_last_n_days

    @property
    def model_name(self):
        return "test_model"

    @property
    def sources(self):
        return {"nssp"}

    @property
    def ed_visit_input_resolution(self):
        return self._ed_visit_input_resolution

    @property
    def minimum_exclude_last_n_days(self):
        return self._minimum_exclude_last_n_days

    def validate_configuration(self):
        self.events.append("validate")

    def prepare_model_artifacts(self, run):
        self.events.append("prepare_artifacts")

    def run_model(self, run):
        self.events.append("run_model")


def _pipeline(
    tmp_path,
    *,
    events=None,
    fail_on_stale_data=False,
    ed_visit_input_resolution="daily",
    minimum_exclude_last_n_days=0,
    exclude_last_n_days=1,
):
    return TestPipeline(
        disease="covid",
        loc="CA",
        output_dir=tmp_path,
        n_lookback_days=90,
        run_date=dt.date(2024, 12, 20),
        exclude_last_n_days=exclude_last_n_days,
        fail_on_stale_data=fail_on_stale_data,
        logger=logging.getLogger("test-forecast-pipeline"),
        events=events,
        ed_visit_input_resolution=ed_visit_input_resolution,
        minimum_exclude_last_n_days=minimum_exclude_last_n_days,
    )


def test_build_forecast_run_loads_inputs_and_constructs_canonical_state(
    monkeypatch, tmp_path
):
    from cfa.stf.routine import forecast_pipeline as pipeline_module

    surveillance = make_test_surveillance_inputs(
        first_training_date=dt.date(2024, 9, 22),
        last_training_date=dt.date(2024, 12, 18),
        sources={"nssp"},
    )
    calls = {}

    def load(**kwargs):
        calls["load"] = kwargs
        return surveillance

    monkeypatch.setattr(pipeline_module, "load_surveillance_inputs", load)

    pipeline = _pipeline(
        tmp_path,
        fail_on_stale_data=True,
        ed_visit_input_resolution="epiweekly",
    )
    run = pipeline.build_forecast_run()

    assert pipeline.requested_window == ForecastWindow(
        report_date=dt.date(2024, 12, 20),
        n_lookback_days=90,
        exclude_last_n_days=1,
    )
    assert run == ForecastRun(
        disease="covid",
        loc="CA",
        forecast_window=ForecastWindow(
            report_date=dt.date(2024, 12, 20),
            n_lookback_days=90,
            exclude_last_n_days=1,
        ),
        model_name="test_model",
        output_dir=tmp_path,
        surveillance=surveillance,
    )
    assert calls["load"]["sources"] == {"nssp"}
    assert calls["load"]["min_allowed_training_date"] == dt.date(2024, 9, 21)
    assert calls["load"]["max_allowed_training_date"] == dt.date(2024, 12, 18)
    assert calls["load"]["ed_visit_input_resolution"] == "epiweekly"
    assert calls["load"]["fail_on_stale_data"] is True
    assert run.model_batch_dir == (tmp_path / "covid_lookback-90_omit-1")
    assert run.model_run_dir == run.model_batch_dir / "model_runs" / "CA"
    assert run.model_dir == run.model_run_dir / "test_model"
    assert run.data_dir == run.model_dir / "data"
    assert run.first_training_date == dt.date(2024, 9, 22)
    assert run.nssp is surveillance.nssp
    assert run.freshness == surveillance.freshness
    assert run.right_truncation_offset == 1
    assert run.forecast_through == dt.date(2025, 1, 11)
    assert run.n_forecast_days == 24


@pytest.mark.parametrize(
    ("report_date", "expected", "expected_days"),
    [
        (dt.date(2026, 9, 2), dt.date(2026, 9, 26), 25),
        (dt.date(2026, 9, 3), dt.date(2026, 9, 26), 24),
        (dt.date(2026, 9, 4), dt.date(2026, 9, 26), 23),
        (dt.date(2026, 9, 5), dt.date(2026, 9, 26), 22),
        (dt.date(2026, 9, 6), dt.date(2026, 10, 3), 28),
        (dt.date(2026, 9, 7), dt.date(2026, 10, 3), 27),
        (dt.date(2026, 9, 8), dt.date(2026, 10, 3), 26),
        (dt.date(2026, 9, 9), dt.date(2026, 10, 3), 25),
    ],
)
def test_forecast_run_forecast_through(tmp_path, report_date, expected, expected_days):
    run = make_test_forecast_run(output_dir=tmp_path, report_date=report_date)

    assert run.forecast_through == expected
    assert run.n_forecast_days == expected_days


@pytest.mark.parametrize(
    (
        "requested_exclusion",
        "expected_exclusion",
        "expected_max_allowed",
    ),
    [
        (1, 4, dt.date(2024, 12, 15)),
        (4, 4, dt.date(2024, 12, 15)),
        (6, 6, dt.date(2024, 12, 13)),
    ],
)
def test_build_forecast_run_applies_minimum_exclusion(
    monkeypatch,
    tmp_path,
    requested_exclusion,
    expected_exclusion,
    expected_max_allowed,
):
    from cfa.stf.routine import forecast_pipeline as pipeline_module

    calls = {}

    def load(**kwargs):
        calls["load"] = kwargs
        return make_test_surveillance_inputs(
            first_training_date=kwargs["min_allowed_training_date"],
            last_training_date=kwargs["max_allowed_training_date"],
            sources={"nssp"},
        )

    monkeypatch.setattr(pipeline_module, "load_surveillance_inputs", load)
    pipeline = _pipeline(
        tmp_path,
        minimum_exclude_last_n_days=4,
        exclude_last_n_days=requested_exclusion,
    )

    run = pipeline.build_forecast_run()

    assert run.first_training_date == dt.date(2024, 9, 21)
    assert run.last_training_date == expected_max_allowed
    assert run.exclude_last_n_days == expected_exclusion
    assert calls["load"]["min_allowed_training_date"] == dt.date(2024, 9, 21)
    assert calls["load"]["max_allowed_training_date"] == expected_max_allowed
    assert run.model_batch_dir == (
        tmp_path / f"covid_lookback-90_omit-{requested_exclusion}"
    )
    assert run.right_truncation_offset == expected_exclusion


def test_model_minimum_does_not_change_batch_directory(monkeypatch, tmp_path):
    from cfa.stf.routine import forecast_pipeline as pipeline_module

    def load(**kwargs):
        return make_test_surveillance_inputs(
            first_training_date=kwargs["min_allowed_training_date"],
            last_training_date=kwargs["max_allowed_training_date"],
            sources={"nssp"},
        )

    monkeypatch.setattr(pipeline_module, "load_surveillance_inputs", load)

    baseline_run = _pipeline(tmp_path).build_forecast_run()
    constrained_run = _pipeline(
        tmp_path,
        minimum_exclude_last_n_days=4,
    ).build_forecast_run()

    assert baseline_run.first_training_date == constrained_run.first_training_date
    assert baseline_run.last_training_date != constrained_run.last_training_date
    assert baseline_run.model_batch_dir == constrained_run.model_batch_dir
    assert baseline_run.model_batch_dir == tmp_path / "covid_lookback-90_omit-1"


def test_execute_runs_lifecycle_in_order(monkeypatch, tmp_path, caplog):
    from cfa.stf.routine import forecast_pipeline as pipeline_module

    events = []
    pipeline = _pipeline(
        tmp_path,
        events=events,
    )
    run = make_test_forecast_run(
        output_dir=tmp_path,
        sources={"nssp"},
    )

    monkeypatch.setattr(
        pipeline,
        "build_forecast_run",
        lambda: events.append("build_run") or run,
    )
    serialize_kwargs = {}

    def serialize(*, forecast_run, logger):
        serialize_kwargs.update(
            forecast_run=forecast_run,
            logger=logger,
        )
        events.append("serialize")

    monkeypatch.setattr(pipeline_module, "serialize_data", serialize)
    monkeypatch.setattr(
        pipeline_module,
        "make_figures_from_model_fit_dir",
        lambda **kwargs: events.append("figures"),
    )
    monkeypatch.setattr(
        pipeline_module,
        "model_fit_dir_to_hub_tbl",
        lambda *args, **kwargs: events.append("hubverse"),
    )

    with caplog.at_level(logging.INFO, logger="test-forecast-pipeline"):
        pipeline.execute()

    assert events == [
        "validate",
        "build_run",
        "serialize",
        "prepare_artifacts",
        "run_model",
        "figures",
        "hubverse",
    ]
    assert serialize_kwargs["forecast_run"] is run
    assert run.data_dir.is_dir()
    messages = [record.getMessage() for record in caplog.records]
    assert messages[0] == (
        "Starting single-location pipeline for model test_model, location CA, "
        "and run date 2024-12-20."
    )


@pytest.mark.parametrize(
    (
        "max_allowed_training_date",
        "exclude_last_n_days",
        "expected_offset",
        "expected_forecast_days",
    ),
    [
        (dt.date(2024, 12, 19), 0, 0, 23),
        (dt.date(2024, 12, 14), 5, 5, 28),
    ],
)
def test_forecast_run_calculates_training_date_offsets(
    tmp_path,
    max_allowed_training_date,
    exclude_last_n_days,
    expected_offset,
    expected_forecast_days,
):
    run = make_test_forecast_run(
        output_dir=tmp_path,
        report_date=dt.date(2024, 12, 20),
        max_allowed_training_date=max_allowed_training_date,
        exclude_last_n_days=exclude_last_n_days,
    )

    assert run.right_truncation_offset == expected_offset
    assert run.n_forecast_days == expected_forecast_days


def test_right_truncation_offset_uses_overall_last_date_with_multiple_sources(
    tmp_path,
):
    run = make_test_forecast_run(
        output_dir=tmp_path,
        report_date=dt.date(2026, 9, 9),
        n_lookback_days=30,
        max_allowed_training_date=dt.date(2026, 9, 7),
        last_training_date=dt.date(2026, 9, 4),
        exclude_last_n_days=1,
    )
    later_nhsn = replace(
        run.nhsn,
        data=run.nhsn.data.with_columns(date=dt.date(2026, 9, 5)),
    )
    run = replace(
        run,
        surveillance=replace(run.surveillance, nhsn=later_nhsn),
    )

    assert run.nssp is not None
    assert run.nhsn is not None
    assert run.nssp.last_training_date == dt.date(2026, 9, 4)
    assert run.nhsn.last_training_date == dt.date(2026, 9, 5)
    assert run.last_training_date == dt.date(2026, 9, 5)
    expected_offset = (run.report_date - run.last_training_date).days - 1
    assert expected_offset == 3
    assert run.right_truncation_offset == expected_offset


def test_right_truncation_offset_uses_nhsn_date_without_nssp(tmp_path):
    run = make_test_forecast_run(output_dir=tmp_path, sources=("nhsn",))

    assert run.right_truncation_offset == 0
