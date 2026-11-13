"""Execution identity, discovery, and downstream orchestration contracts."""

import datetime as dt
import json
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock

import polars as pl
import pytest
from pypdf import PdfReader, PdfWriter

from cfa.stf.routine import fusion as fusion_module
from cfa.stf.routine.forecast_window import ForecastWindow
from cfa.stf.routine.fusion import (
    FUSION_SPECS,
    materialize_fusion,
    validate_fusion_sources,
)
from cfa.stf.routine.run_storage import (
    FitKey,
    dagster_execution_directory,
    execution_directory,
    find_fits,
    write_json,
)
from cfa.stf.routine.utils import postprocess_forecast_batches as postprocess
from cfa.stf.routine.utils.collate_plots import collect_pdfs

START = dt.datetime(2026, 9, 2, 12, tzinfo=dt.UTC)


def execution(base, id, hour=0):
    return execution_directory(base, START + dt.timedelta(hours=hour), id, {})


def fit(directory, model, value=1.0, location="CA", complete=True, disease="covid"):
    path = directory / location / disease / model
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


def test_find_fits_falls_back_when_execution_is_renamed(tmp_path):
    older = execution(tmp_path, "old")
    newer = execution(tmp_path, "new", 1)
    first = fit(older, "pyrenew_e")
    latest = fit(newer, "pyrenew_e")
    assert find_fits(tmp_path)[("CA", "covid", "pyrenew_e")] == latest
    newer.rename(tmp_path / "disabled")
    assert find_fits(tmp_path)[("CA", "covid", "pyrenew_e")] == first


@pytest.mark.parametrize(
    "newer_name,filtered",
    [
        ("2026-09-02-09-00-00_a", False),
        ("2026-09-02-08-00-00_z", False),
        ("2026-09-02-09-00-00_a", True),
    ],
    ids=["timestamp-order", "execution-id-tiebreak", "requested-keys"],
)
@pytest.mark.parametrize("keep", ["all", "earliest", "latest"])
def test_find_fits_selects_completed_inputs_by_directory_name(
    tmp_path, newer_name, filtered, keep
):
    older = tmp_path / "2026-09-02-08-00-00_a"
    newer = tmp_path / newer_name
    first = fit(older, "pyrenew_e")
    last = fit(newer, "pyrenew_e")
    # Neither malformed nor absent execution metadata should affect selection.
    (older / "execution.json").write_text("invalid JSON")
    fit(tmp_path / "2026-09-02-10-00-00_incomplete", "pyrenew_e", complete=False)
    other = fit(newer, "daily_fable_e_other")
    unrelated = fit(older, "pyrenew_e", location="TX")
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
        samples = pl.DataFrame(
            {
                "date": pl.date_range(
                    dt.date(2026, 9, 2), dt.date(2026, 9, 26), eager=True
                ),
                ".draw": 1,
                ".value": 0.3,
                "resolution": "daily",
            }
        )
        samples.write_parquet(kwargs["destination_dir"] / "samples.parquet")
        return samples

    monkeypatch.setattr(fusion_module, "create_prop_fusion", fuse)

    def publish(path, **kwargs):
        pl.DataFrame(
            {"reference_date": [dt.date(2026, 9, 2)], "value": [3.0]}
        ).write_parquet(path / "hubverse_table.parquet")

    monkeypatch.setattr(fusion_module, "postprocess_model_fit_dir", publish)
    spec = FUSION_SPECS[0]
    inputs = find_fits(tmp_path, keys=spec.input_keys("CA", "covid"))
    materialize_fusion(spec, inputs, downstream, "CA", "covid")
    postprocess.main(
        tmp_path, forecast_date=dt.date(2026, 9, 2), execution_dir=downstream
    )
    assert len(calls) == 1
    output = (
        downstream / "00_summary/hub_tables/2026-09-02-covid-hubverse-table.parquet"
    )
    assert pl.read_parquet(output).height == 3
    fusion = downstream / "CA/covid/prop_pyrenew_e_daily_fable_e_other"
    sources = pl.read_csv(downstream / "00_summary/sources.tsv", separator="\t")
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


@pytest.mark.parametrize("diseases", [[], ["covid", "flu"]])
def test_postprocess_discovers_all_completed_diseases(tmp_path, diseases):
    first = execution(tmp_path, "first")
    for disease in diseases:
        for location in ("CA", "US"):
            path = fit(first, "daily_fable_e_other", location=location, disease=disease)
            figures = path / "figures"
            figures.mkdir()
            with PdfWriter() as writer:
                writer.add_blank_page(width=72, height=72)
                writer.write(figures / "forecast.pdf")
    fit(first, "daily_fable_e_other", disease="rsv", complete=False)
    destination = execution(tmp_path, "summary", 1)

    postprocess.main(
        tmp_path, forecast_date=dt.date(2026, 9, 2), execution_dir=destination
    )

    summary = destination / "00_summary"
    sources = pl.read_csv(summary / "sources.tsv", separator="\t")
    assert set(sources["disease"]) == set(diseases)
    assert {path.name for path in summary.iterdir()} == {
        "sources.tsv",
        "figures",
        "hub_tables",
    }
    for disease in diseases:
        table = pl.read_parquet(
            summary / "hub_tables" / f"2026-09-02-{disease}-hubverse-table.parquet"
        )
        assert table.height == 2
        figure = summary / "figures" / disease / "forecast.pdf"
        assert len(PdfReader(figure).pages) == 2


def test_incompatible_horizons_fail(tmp_path):
    run = execution(tmp_path, "run")
    fit(run, "pyrenew_e")
    other = fit(run, "daily_fable_e_other")
    write_json(
        other / "metadata.json",
        {"forecast_date": "2026-09-02", "forecast_through": "2026-10-03"},
    )
    with pytest.raises(ValueError, match="windows differ"):
        materialize_fusion(FUSION_SPECS[0], find_fits(tmp_path), run, "CA", "covid")


@pytest.mark.parametrize(
    "resolution,outcome",
    [
        ("daily", "success"),
        ("epiweekly", "success"),
        ("daily", "missing_date"),
        ("epiweekly", "missing_date"),
        ("daily", "postprocessing_failure"),
    ],
)
def test_fusion_validates_every_draw_before_publication(
    tmp_path, monkeypatch, resolution, outcome
):
    run = execution(tmp_path, "run")
    spec = FUSION_SPECS[0] if resolution == "daily" else FUSION_SPECS[1]
    fit(run, spec.numerator)
    fit(run, spec.other)
    destination = fit(run, spec.model_name)
    dates = pl.date_range(
        dt.date(2026, 9, 2) if resolution == "daily" else dt.date(2026, 9, 5),
        dt.date(2026, 9, 26),
        interval="1d" if resolution == "daily" else "1w",
        eager=True,
    ).to_list()
    second_draw_dates = dates[:-1] if outcome == "missing_date" else dates

    def fuse(**kwargs):
        assert not (destination / "_COMPLETE").exists()
        samples = pl.DataFrame(
            {
                "date": dates + second_draw_dates,
                ".draw": [1] * len(dates) + [2] * len(second_draw_dates),
                ".value": 0.3,
                "resolution": resolution,
            }
        )
        samples.write_parquet(kwargs["destination_dir"] / "samples.parquet")
        return samples

    def publication(*args, **kwargs):
        assert not (destination / "_COMPLETE").exists()
        if outcome == "postprocessing_failure":
            raise OSError("Simulated publication failure")

    publish = Mock(side_effect=publication)
    monkeypatch.setattr(fusion_module, "create_prop_fusion", fuse)
    monkeypatch.setattr(fusion_module, "postprocess_model_fit_dir", publish)

    if outcome == "missing_date":
        with pytest.raises(ValueError, match="required forecast dates"):
            materialize_fusion(spec, find_fits(tmp_path), run, "CA", "covid")
        publish.assert_not_called()
    elif outcome.endswith("_failure"):
        with pytest.raises(OSError, match="Simulated publication failure"):
            materialize_fusion(spec, find_fits(tmp_path), run, "CA", "covid")
        publish.assert_called_once_with(destination, forecast_date=dt.date(2026, 9, 2))

    if outcome != "success":
        assert not (destination / "_COMPLETE").exists()
        assert (destination / "samples.parquet").is_file()
        assert spec.model_name not in {key.model for key in find_fits(tmp_path)}
    else:
        assert (
            materialize_fusion(spec, find_fits(tmp_path), run, "CA", "covid")
            == destination
        )
        publish.assert_called_once_with(destination, forecast_date=dt.date(2026, 9, 2))
        assert (destination / "_COMPLETE").is_file()


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


@pytest.mark.parametrize(
    "changed",
    [
        "numerator",
        "other",
        "missing_numerator",
        "missing_other",
        "missing_sources",
        "incomplete_sources",
        "swapped_sources",
        "unrelated",
    ],
)
def test_summaries_reject_stale_fusion_without_recomputing(
    tmp_path, monkeypatch, changed
):
    initial = execution(tmp_path, "first")
    numerator = fit(initial, "pyrenew_e")
    other = fit(initial, "daily_fable_e_other")
    fusion = fit(initial, FUSION_SPECS[0].model_name, value=3.0)
    write_json(fusion / "metadata.json", {"sources": [str(numerator), str(other)]})
    newer = execution(tmp_path, "new", 1)
    if changed == "missing_numerator":
        (numerator / "_COMPLETE").unlink()
    elif changed == "missing_other":
        (other / "_COMPLETE").unlink()
    elif changed == "missing_sources":
        write_json(fusion / "metadata.json", {})
    elif changed == "incomplete_sources":
        write_json(fusion / "metadata.json", {"sources": [str(numerator)]})
    elif changed == "swapped_sources":
        write_json(fusion / "metadata.json", {"sources": [str(other), str(numerator)]})
    else:
        model = {
            "numerator": "pyrenew_e",
            "other": "daily_fable_e_other",
            "unrelated": "pyrenew_h",
        }[changed]
        fit(newer, model, value=10.0)

    def unexpected_fusion(**kwargs):
        pytest.fail("Postprocessing must never execute fusion")

    monkeypatch.setattr(fusion_module, "create_prop_fusion", unexpected_fusion)
    snapshot = find_fits(tmp_path)
    original = snapshot.copy()
    if changed == "unrelated":
        validate_fusion_sources(snapshot)
        postprocess.main(
            tmp_path, forecast_date=dt.date(2026, 9, 2), execution_dir=newer
        )
        values = pl.read_parquet(
            newer / "00_summary/hub_tables/2026-09-02-covid-hubverse-table.parquet"
        )["value"].to_list()
        assert 3.0 in values
        collect_pdfs(tmp_path)
    else:
        with pytest.raises(ValueError, match="Stale fusion") as error:
            validate_fusion_sources(snapshot)
        assert str(fusion) in str(error.value)
        assert "recorded sources" in str(error.value)
        assert "latest completed inputs" in str(error.value)
        assert "Rerun the fusion" in str(error.value)
        with pytest.raises(ValueError, match="Stale fusion"):
            postprocess.main(
                tmp_path, forecast_date=dt.date(2026, 9, 2), execution_dir=newer
            )
        with pytest.raises(ValueError, match="Stale fusion"):
            collect_pdfs(tmp_path)
        assert not (newer / "00_summary").exists()
    assert snapshot == original


def test_summaries_reject_unknown_fusion_recipe(tmp_path):
    run = execution(tmp_path, "run")
    fusion = fit(run, "prop_unknown")

    with pytest.raises(ValueError, match="Unknown fusion recipe") as error:
        postprocess.main(tmp_path, forecast_date=dt.date(2026, 9, 2), execution_dir=run)
    assert str(fusion) in str(error.value)
    with pytest.raises(ValueError, match="Unknown fusion recipe"):
        collect_pdfs(tmp_path)
    assert not (run / "00_summary").exists()


def test_fusion_missing_inputs_fails_without_marking_complete(tmp_path):
    run = execution(tmp_path, "first")
    fit(run, "pyrenew_e")
    with pytest.raises(FileNotFoundError, match="Missing completed fusion inputs"):
        materialize_fusion(FUSION_SPECS[0], find_fits(tmp_path), run, "CA", "covid")
    assert not (run / "CA/covid" / FUSION_SPECS[0].model_name / "_COMPLETE").exists()
