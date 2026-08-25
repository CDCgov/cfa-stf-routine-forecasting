import datetime as dt
import logging

import pytest

from cfa.stf.routine.forecast_pipeline import ForecastPipeline
from cfa.stf.routine.forecast_run import ForecastRun
from tests.factories import make_test_forecast_run, make_test_surveillance_inputs


class TestPipeline(ForecastPipeline):
    __test__ = False

    def __init__(self, *, events=None, ed_visit_input_resolution="daily", **kwargs):
        super().__init__(**kwargs)
        self.events = events if events is not None else []
        self._ed_visit_input_resolution = ed_visit_input_resolution

    @property
    def model_name(self):
        return "test_model"

    @property
    def sources(self):
        return {"nssp"}

    @property
    def ed_visit_input_resolution(self):
        return self._ed_visit_input_resolution

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
):
    return TestPipeline(
        disease="covid",
        loc="CA",
        output_dir=tmp_path,
        n_training_days=90,
        n_forecast_days=28,
        run_date=dt.date(2024, 12, 20),
        exclude_last_n_days=1,
        fail_on_stale_data=fail_on_stale_data,
        logger=logging.getLogger("test-forecast-pipeline"),
        events=events,
        ed_visit_input_resolution=ed_visit_input_resolution,
    )


def test_build_forecast_run_loads_inputs_and_constructs_canonical_state(
    monkeypatch, tmp_path
):
    from cfa.stf.routine import forecast_pipeline as pipeline_module

    surveillance = make_test_surveillance_inputs(
        last_training_date=dt.date(2024, 12, 18),
        sources={"nssp"},
    )
    calls = {}

    def calculate(*args):
        calls["calculate"] = args
        return dt.date(2024, 9, 20), dt.date(2024, 12, 18)

    def load(**kwargs):
        calls["load"] = kwargs
        return surveillance

    monkeypatch.setattr(pipeline_module, "calculate_training_dates", calculate)
    monkeypatch.setattr(pipeline_module, "load_surveillance_inputs", load)

    pipeline = _pipeline(
        tmp_path,
        fail_on_stale_data=True,
        ed_visit_input_resolution="epiweekly",
    )
    run = pipeline.build_forecast_run()

    assert run == ForecastRun(
        disease="covid",
        loc="CA",
        report_date=dt.date(2024, 12, 20),
        first_training_date=dt.date(2024, 9, 20),
        last_training_date=dt.date(2024, 12, 18),
        n_forecast_days=28,
        exclude_last_n_days=1,
        model_name="test_model",
        output_dir=tmp_path,
        surveillance=surveillance,
    )
    assert calls["calculate"][:3] == (
        dt.date(2024, 12, 20),
        90,
        1,
    )
    assert calls["load"]["sources"] == {"nssp"}
    assert calls["load"]["ed_visit_input_resolution"] == "epiweekly"
    assert calls["load"]["fail_on_stale_data"] is True
    assert run.model_batch_dir == (
        tmp_path / "covid_r_2024-12-20_f_2024-09-20_t_2024-12-18"
    )
    assert run.model_run_dir == run.model_batch_dir / "model_runs" / "CA"
    assert run.model_dir == run.model_run_dir / "test_model"
    assert run.data_dir == run.model_dir / "data"
    assert run.nssp is surveillance.nssp
    assert run.freshness == surveillance.freshness
    assert run.right_truncation_offset == 1


def test_execute_runs_lifecycle_in_order(monkeypatch, tmp_path, caplog):
    from cfa.stf.routine import forecast_pipeline as pipeline_module

    events = []
    pipeline = _pipeline(
        tmp_path,
        events=events,
        ed_visit_input_resolution="epiweekly",
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
        lambda *args: events.append("hubverse"),
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
    ("last_training_date", "exclude_last_n_days", "expected_offset"),
    [
        (dt.date(2024, 12, 19), 0, 0),
        (dt.date(2024, 12, 14), 5, 5),
    ],
)
def test_forecast_run_calculates_right_truncation_offset(
    tmp_path,
    last_training_date,
    exclude_last_n_days,
    expected_offset,
):
    run = make_test_forecast_run(
        output_dir=tmp_path,
        report_date=dt.date(2024, 12, 20),
        last_training_date=last_training_date,
        exclude_last_n_days=exclude_last_n_days,
    )

    assert run.right_truncation_offset == expected_offset
