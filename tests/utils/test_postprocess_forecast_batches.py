"""Execution identity, discovery, and downstream orchestration contracts."""

import datetime as dt
import json
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from types import SimpleNamespace

import polars as pl
import pytest

from cfa.stf.routine import fusion as fusion_module
from cfa.stf.routine.forecast_window import ForecastWindow
from cfa.stf.routine.fusion import FUSION_SPECS, current_fits, run_fusion
from cfa.stf.routine.run_storage import (
    FitKey,
    dagster_execution_directory,
    execution_directory,
    find_fits,
    write_json,
)
from cfa.stf.routine.utils import postprocess_forecast_batches as postprocess

START = dt.datetime(2026, 9, 2, 12, tzinfo=dt.UTC)


def execution(base, id, hour=0):
    return execution_directory(base, START + dt.timedelta(hours=hour), id, {})


def fit(directory, model, value=1.0, location="CA", complete=True):
    path = directory / location / "covid" / model
    path.mkdir(parents=True, exist_ok=True)
    write_json(
        path / "metadata.json",
        {
            "forecast_date": dt.date(2026, 9, 2),
            "forecast_through": dt.date(2026, 9, 26),
        },
    )
    pl.DataFrame(
        {"reference_date": [dt.date(2026, 9, 2)], "value": [value]}
    ).write_parquet(path / "hubverse_table.parquet")
    if complete:
        (path / "_COMPLETE").touch()
    return path


def test_write_json_serializes_forecast_metadata(tmp_path):
    path = tmp_path / "metadata.json"
    write_json(
        path,
        {
            "window": ForecastWindow(dt.date(2026, 9, 2), n_lookback_days=150),
            "source": tmp_path / "source",
        },
    )
    assert json.loads(path.read_text()) == {
        "window": {
            "forecast_date": "2026-09-02",
            "n_lookback_days": 150,
            "exclude_last_n_days": 0,
        },
        "source": str(tmp_path / "source"),
    }
    assert list(tmp_path.iterdir()) == [path]


@pytest.mark.parametrize("operation", ["write_bytes", "replace"])
def test_failed_metadata_write_preserves_destination_and_cleans_staging(
    tmp_path, monkeypatch, operation
):
    path = tmp_path / "metadata.json"
    write_json(path, {"original": True})

    def fail(*args, **kwargs):
        raise OSError("Simulated storage failure")

    monkeypatch.setattr(Path, operation, fail)
    with pytest.raises(OSError, match="Simulated storage failure"):
        write_json(path, {"original": False})

    assert json.loads(path.read_text()) == {"original": True}
    assert list(tmp_path.iterdir()) == [path]


def test_identity_shared_across_workers_and_collision(tmp_path):
    with ThreadPoolExecutor(max_workers=8) as pool:
        paths = list(pool.map(lambda _: execution(tmp_path, "same"), range(16)))
    assert len(set(paths)) == 1
    assert paths[0].name == "2026-09-02-08-00-00_same"
    assert sorted(entry.name for entry in paths[0].iterdir()) == [
        "execution.json",
        "run_config.yaml",
    ]
    second = execution(tmp_path, "different")
    assert second != paths[0]
    assert second.name == "2026-09-02-08-00-00_different"
    assert execution(tmp_path, "different") == second
    assert (second / "run_config.yaml").exists()


def test_dagster_identity_uses_persisted_start(tmp_path):
    context = SimpleNamespace(
        run_id="run",
        run=SimpleNamespace(run_config={"ops": {}}),
        instance=SimpleNamespace(
            get_run_record_by_id=lambda _: SimpleNamespace(start_time=START.timestamp())
        ),
    )
    assert dagster_execution_directory(
        context, tmp_path
    ) == dagster_execution_directory(context, tmp_path)
    assert (tmp_path / "2026-09-02-08-00-00_run").is_dir()


@pytest.mark.parametrize(
    "started, name, stored",
    [
        (
            "2026-01-02T17:00:00+00:00",
            "2026-01-02-12-00-00_run",
            "2026-01-02T12:00:00-05:00",
        ),
        (
            "2026-09-02T16:00:00+00:00",
            "2026-09-02-12-00-00_run",
            "2026-09-02T12:00:00-04:00",
        ),
    ],
)
def test_execution_timestamp_uses_new_york(tmp_path, started, name, stored):
    directory = execution_directory(
        tmp_path, dt.datetime.fromisoformat(started), "run", {}
    )
    assert directory.name == name
    assert (
        json.loads((directory / "execution.json").read_text())["started_at"] == stored
    )


def test_latest_completed_per_location_and_renamed_run(tmp_path):
    older = execution(tmp_path, "old")
    newer = execution(tmp_path, "new", 1)
    first = fit(older, "pyrenew_e")
    other_location = fit(older, "pyrenew_e", location="TX")
    fit(newer, "pyrenew_e", complete=False)
    assert find_fits(tmp_path)[("CA", "covid", "pyrenew_e")] == first
    latest = fit(newer, "pyrenew_e")
    snapshot = find_fits(tmp_path)
    assert snapshot[("CA", "covid", "pyrenew_e")] == latest
    assert snapshot[("TX", "covid", "pyrenew_e")] == other_location
    newer.rename(tmp_path / "disabled")
    assert find_fits(tmp_path)[("CA", "covid", "pyrenew_e")] == first
    assert snapshot[("CA", "covid", "pyrenew_e")] == latest


@pytest.mark.parametrize(
    "newer_name", ["2026-09-02-09-00-00_a", "2026-09-02-08-00-00_z"]
)
@pytest.mark.parametrize("keep", ["all", "earliest", "latest"])
def test_find_fits_uses_directory_names_without_execution_metadata(
    tmp_path, newer_name, keep
):
    older = tmp_path / "2026-09-02-08-00-00_a"
    newer = tmp_path / newer_name
    latest = fit(newer, "pyrenew_e")
    first = fit(older, "pyrenew_e")
    (older / "execution.json").write_text("invalid JSON")

    expected = {"all": [first, latest], "earliest": first, "latest": latest}
    assert find_fits(tmp_path, keep=keep) == {
        FitKey("CA", "covid", "pyrenew_e"): expected[keep]
    }


@pytest.mark.parametrize("keep", ["all", "earliest", "latest"])
@pytest.mark.parametrize("filtered", [False, True])
def test_find_fits_retains_requested_executions(tmp_path, keep, filtered):
    older = execution(tmp_path, "older")
    newer = execution(tmp_path, "newer", 1)
    incomplete = execution(tmp_path, "incomplete", 2)
    first = fit(older, "pyrenew_e")
    last = fit(newer, "pyrenew_e")
    fit(incomplete, "pyrenew_e", complete=False)
    other = fit(newer, "daily_fable_e_other")
    unrelated = fit(newer, "pyrenew_e", location="TX")
    disabled = tmp_path / "disabled"
    fit(disabled, "pyrenew_e")
    numerator_key, other_key = FUSION_SPECS[0].input_keys("CA", "covid")
    keys = [numerator_key, other_key, FitKey("CA", "covid", "missing")]
    expected = {
        numerator_key: {"all": [first, last], "earliest": first, "latest": last}[keep],
        other_key: [other] if keep == "all" else other,
    }
    if not filtered:
        expected[FitKey("TX", "covid", "pyrenew_e")] = (
            [unrelated] if keep == "all" else unrelated
        )
    assert find_fits(tmp_path, keys=keys if filtered else None, keep=keep) == expected


@pytest.mark.parametrize("include_missing", [False, True])
def test_find_fits_looks_up_only_requested_inputs(
    tmp_path, monkeypatch, include_missing
):
    oldest = execution(tmp_path, "oldest", -1)
    older = execution(tmp_path, "older")
    newer = execution(tmp_path, "newer", 1)
    keys = FUSION_SPECS[0].input_keys("CA", "covid")
    numerator = fit(older, "pyrenew_e")
    fit(newer, "pyrenew_e", complete=False)
    fit(older, "daily_fable_e_other")
    other = fit(newer, "daily_fable_e_other")
    fit(newer, "pyrenew_e", location="TX")
    fit(newer, "pyrenew_h")
    fit(oldest, "pyrenew_e")
    fit(oldest, "daily_fable_e_other")
    disabled = tmp_path / "disabled"
    fit(disabled, "pyrenew_e")
    fit(disabled, "daily_fable_e_other")
    requested = (*keys, FitKey("CA", "covid", "missing")) if include_missing else keys
    inspected = []
    is_file = Path.is_file

    def inspect_marker(path):
        inspected.append(path)
        return is_file(path)

    def unexpected_glob(*args, **kwargs):
        pytest.fail("Requested inputs must be looked up without scanning all fits")

    monkeypatch.setattr(Path, "is_file", inspect_marker)
    monkeypatch.setattr(Path, "glob", unexpected_glob)
    assert find_fits(tmp_path, keys=iter(requested)) == {
        keys[0]: numerator,
        keys[1]: other,
    }
    assert {FitKey(*path.parent.parts[-3:]) for path in inspected} <= set(requested)
    assert not any(path.is_relative_to(disabled) for path in inspected)
    if not include_missing:
        assert not any(path.is_relative_to(oldest) for path in inspected)


@pytest.mark.parametrize("keep", ["all", "earliest", "latest"])
def test_find_fits_with_no_requested_keys(tmp_path, keep):
    fit(execution(tmp_path, "run"), "pyrenew_e")
    assert find_fits(tmp_path, keys=[], keep=keep) == {}


def test_find_fits_rejects_unknown_keep(tmp_path):
    with pytest.raises(ValueError, match="keep must be"):
        find_fits(tmp_path, keep="newest")


@pytest.mark.parametrize("separate", [False, True])
def test_fusion_uses_executing_run_and_latest_sources(tmp_path, monkeypatch, separate):
    first = execution(tmp_path, "first")
    second = execution(tmp_path, "second", 1) if separate else first
    downstream = execution(tmp_path, "downstream", 2) if separate else first
    numerator = fit(first, "pyrenew_e")
    other = fit(second, "daily_fable_e_other")
    calls = []

    def fuse(**kwargs):
        calls.append(kwargs)
        assert kwargs["numerator_dir"] == numerator
        assert kwargs["other_dir"] == other
        assert kwargs["destination_dir"].parents[2] == downstream

    monkeypatch.setattr(fusion_module, "create_prop_fusion_model", fuse)
    monkeypatch.setattr(
        fusion_module, "make_figures_from_model_fit_dir", lambda *a, **k: None
    )

    def publish(path, **kwargs):
        pl.DataFrame(
            {"reference_date": [dt.date(2026, 9, 2)], "value": [3.0]}
        ).write_parquet(path / "hubverse_table.parquet")

    monkeypatch.setattr(fusion_module, "model_fit_dir_to_hub_tbl", publish)
    spec = FUSION_SPECS[0]
    inputs = find_fits(tmp_path, keys=spec.input_keys("CA", "covid"))
    run_fusion(spec, inputs, downstream, "CA", "covid")
    postprocess.main(tmp_path, ["covid"], execution_dir=downstream)
    assert len(calls) == 1
    output = downstream / "summary/covid/2026-09-02-covid-hubverse-table.parquet"
    assert pl.read_parquet(output).height == 3
    fusion = downstream / "CA/covid/prop_pyrenew_e_daily_fable_e_other"
    sources = pl.read_csv(downstream / "summary/sources.tsv", separator="\t")
    assert sources.columns == ["location", "disease", "model", "path"]
    assert sources.rows() == sorted(
        [
            ("CA", "covid", spec.numerator, str(numerator)),
            ("CA", "covid", spec.other, str(other)),
            ("CA", "covid", spec.model_name, str(fusion)),
        ]
    )
    assert (fusion / "_COMPLETE").read_bytes() == b""
    assert json.loads((fusion / "metadata.json").read_text())["sources"] == [
        str(numerator),
        str(other),
    ]


def test_incompatible_horizons_fail(tmp_path):
    run = execution(tmp_path, "run")
    fit(run, "pyrenew_e")
    other = fit(run, "daily_fable_e_other")
    write_json(
        other / "metadata.json",
        {"forecast_date": "2026-09-02", "forecast_through": "2026-10-03"},
    )
    with pytest.raises(ValueError, match="windows differ"):
        run_fusion(FUSION_SPECS[0], find_fits(tmp_path), run, "CA", "covid")


def test_real_dagster_run_shares_identity_across_dependent_assets(tmp_path):
    import dagster as dg

    paths = []

    @dg.asset
    def base_model(context):
        paths.append(dagster_execution_directory(context, tmp_path))

    @dg.asset(deps=[base_model])
    def fusion(context):
        paths.append(dagster_execution_directory(context, tmp_path))

    result = dg.materialize([base_model, fusion])
    assert result.success
    assert len(paths) == 2
    assert paths[0] == paths[1]


@pytest.mark.parametrize("changed", ["numerator", "other", "missing", "unrelated"])
def test_summaries_exclude_stale_fusion_without_recomputing(
    tmp_path, monkeypatch, changed
):
    initial = execution(tmp_path, "first")
    numerator = fit(initial, "pyrenew_e")
    other = fit(initial, "daily_fable_e_other")
    fusion = fit(initial, FUSION_SPECS[0].model_name, value=3.0)
    write_json(fusion / "metadata.json", {"sources": [str(numerator), str(other)]})
    newer = execution(tmp_path, "new", 1)
    if changed == "missing":
        (other / "_COMPLETE").unlink()
    else:
        model = {
            "numerator": "pyrenew_e",
            "other": "daily_fable_e_other",
            "unrelated": "pyrenew_h",
        }[changed]
        fit(newer, model, value=10.0)

    def unexpected_fusion(**kwargs):
        pytest.fail("Postprocessing must never execute fusion")

    monkeypatch.setattr(fusion_module, "create_prop_fusion_model", unexpected_fusion)
    snapshot = find_fits(tmp_path)
    key = FitKey("CA", "covid", FUSION_SPECS[0].model_name)
    assert (key in current_fits(snapshot)) == (changed == "unrelated")
    postprocess.main(tmp_path, ["covid"], execution_dir=newer)
    values = pl.read_parquet(
        newer / "summary/covid/2026-09-02-covid-hubverse-table.parquet"
    )["value"].to_list()
    assert (3.0 in values) == (changed == "unrelated")
    assert key in snapshot  # Filtering must not mutate the discovery snapshot.


def test_fusion_missing_inputs_fails_without_marking_complete(tmp_path):
    run = execution(tmp_path, "first")
    fit(run, "pyrenew_e")
    with pytest.raises(FileNotFoundError, match="Missing completed fusion inputs"):
        run_fusion(FUSION_SPECS[0], find_fits(tmp_path), run, "CA", "covid")
    assert not (run / "CA/covid" / FUSION_SPECS[0].model_name / "_COMPLETE").exists()
