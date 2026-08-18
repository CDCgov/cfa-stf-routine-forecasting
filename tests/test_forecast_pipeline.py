import datetime as dt
import logging

from cfa.stf.routine.forecast_pipeline import ForecastPipeline
from cfa.stf.routine.forecast_run import ForecastRun
from tests.factories import make_test_surveillance_inputs


class TestPipeline(ForecastPipeline[str]):
    __test__ = False

    def __init__(self, *, events=None, **kwargs):
        super().__init__(**kwargs)
        self.events = events if events is not None else []

    @property
    def model_name(self):
        return "test_model"

    @property
    def sources(self):
        return {"nssp"}

    def validate_configuration(self):
        self.events.append("validate")

    def resolve_model_inputs(self, run):
        self.events.append("resolve")
        return "resolved_model_inputs"

    def before_data_preparation(self, run):
        self.events.append("before_prepare")

    def after_data_serialization(self, run, model_inputs):
        assert model_inputs == "resolved_model_inputs"
        self.events.append("after_serialize")

    def fit_and_forecast(self, run, model_inputs):
        assert model_inputs == "resolved_model_inputs"
        self.events.append("forecast")

    def before_post_process(self, run):
        self.events.append("before_postprocess")


def _pipeline(tmp_path, *, events=None, fail_on_stale_data=False):
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
    )


def test_build_forecast_run_loads_inputs_and_constructs_canonical_state(
    monkeypatch, tmp_path
):
    from cfa.stf.routine import forecast_pipeline as pipeline_module

    surveillance = make_test_surveillance_inputs(sources={"nssp"})
    calls = {}

    def calculate(*args):
        calls["calculate"] = args
        return dt.date(2024, 9, 22), dt.date(2024, 12, 19)

    def load(**kwargs):
        calls["load"] = kwargs
        return surveillance

    monkeypatch.setattr(pipeline_module, "calculate_training_dates", calculate)
    monkeypatch.setattr(pipeline_module, "load_surveillance_inputs", load)

    pipeline = _pipeline(tmp_path, fail_on_stale_data=True)
    run = pipeline.build_forecast_run()

    assert run == ForecastRun(
        disease="covid",
        loc="CA",
        report_date=dt.date(2024, 12, 20),
        first_training_date=dt.date(2024, 9, 22),
        last_training_date=dt.date(2024, 12, 19),
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
    assert calls["load"]["fail_on_stale_data"] is True
    assert run.model_batch_dir == (
        tmp_path / "covid_r_2024-12-20_f_2024-09-22_t_2024-12-19"
    )
    assert run.model_run_dir == run.model_batch_dir / "model_runs" / "CA"
    assert run.model_dir == run.model_run_dir / "test_model"
    assert run.data_dir == run.model_dir / "data"
    assert run.nssp is surveillance.nssp
    assert run.freshness == surveillance.freshness
    assert run.right_truncation_offset == 0


def test_execute_runs_lifecycle_in_order(monkeypatch, tmp_path):
    from cfa.stf.routine import forecast_pipeline as pipeline_module

    events = []
    pipeline = _pipeline(tmp_path, events=events)
    run = ForecastRun(
        disease="covid",
        loc="CA",
        report_date=dt.date(2024, 12, 20),
        first_training_date=dt.date(2024, 9, 22),
        last_training_date=dt.date(2024, 12, 20),
        n_forecast_days=28,
        exclude_last_n_days=0,
        model_name="test_model",
        output_dir=tmp_path,
        surveillance=make_test_surveillance_inputs(sources={"nssp"}),
    )

    monkeypatch.setattr(
        pipeline,
        "build_forecast_run",
        lambda: events.append("build_run") or run,
    )
    monkeypatch.setattr(
        pipeline_module,
        "serialize_data",
        lambda **kwargs: events.append("serialize"),
    )
    monkeypatch.setattr(
        pipeline_module,
        "append_prop_data_to_combined_data",
        lambda *args: events.append("append_prop"),
    )
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

    pipeline.execute()

    assert events == [
        "validate",
        "build_run",
        "resolve",
        "before_prepare",
        "serialize",
        "after_serialize",
        "append_prop",
        "forecast",
        "before_postprocess",
        "figures",
        "hubverse",
    ]
    assert run.data_dir.is_dir()
