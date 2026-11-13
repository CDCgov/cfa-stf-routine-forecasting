import datetime as dt
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, call

import dagster as dg
import pytest

from cfa.stf.routine.dagster.defs import asset_config, asset_helpers, assets
from cfa.stf.routine.run_storage import FitKey


def _model_base_config(
    override_fields: dict[str, object],
    *,
    fail_on_stale_data: bool = True,
) -> asset_config.ModelBaseConfig:
    config = asset_config.ModelBaseConfig(
        exclude_last_n_days=1,
        fail_on_stale_data=fail_on_stale_data,
        config_overrides=[
            asset_config.ConfigOverride(
                location="CA",
                exclude_last_n_days=2,
                **override_fields,
            ).as_dict()
        ],
    )
    config.diseases._current_value = "covid"
    config.locations._current_value = "CA"
    return config


def test_launchpad_has_model_defaults_and_shared_location_override():
    base_fields = asset_config.ModelBaseConfig.model_fields
    override_fields = asset_config.ConfigOverride.model_fields

    assert "n_lookback_days" not in base_fields
    assert "n_lookback_days" in override_fields
    assert "fail_on_stale_data" in base_fields
    assert "fail_on_stale_data" not in override_fields
    assert "output_basedir" not in override_fields
    assert "output_basedir" not in base_fields
    assert "fable_pyrenew_n_lookback_days" not in override_fields
    assert "nowcastautogp_n_lookback_days" not in override_fields
    assert "fable_pyrenew_n_lookback_days" not in base_fields
    assert "nowcastautogp_n_lookback_days" not in base_fields
    assert asset_config.FableEOtherConfig().n_lookback_days == 150
    assert asset_config.PyrenewConfig().n_lookback_days == 150
    expected_nowcastautogp = None if asset_config.is_prod() else 150
    assert (
        asset_config.NowcastAutoGPEPctEpiweeklyConfig().n_lookback_days
        == expected_nowcastautogp
    )


@pytest.mark.parametrize("fail_on_stale_data", [False, True])
def test_location_lookback_override_preserves_other_locations_and_model_config(
    fail_on_stale_data,
):
    base = _model_base_config(
        {"n_lookback_days": None}, fail_on_stale_data=fail_on_stale_data
    )
    model = asset_config.PyrenewConfig(n_lookback_days=120)

    assert base.apply_location_overrides("CA", model).n_lookback_days is None
    assert base.apply_location_overrides("US", model).n_lookback_days == 120
    assert model.n_lookback_days == 120
    assert base.get_by_location("US").exclude_last_n_days == 1
    assert base.get_by_location("CA").exclude_last_n_days == 2
    assert base.get_by_location("CA").fail_on_stale_data is fail_on_stale_data
    assert base.get_by_location("US").fail_on_stale_data is fail_on_stale_data


def test_postprocess_uses_executing_run(monkeypatch):
    monkeypatch.setattr(
        assets, "dagster_execution_directory", lambda context, base: base / "run"
    )
    postprocess = Mock()
    monkeypatch.setattr(assets, "_throw_if_backfill", Mock())
    monkeypatch.setattr(assets, "postprocess", postprocess)

    with dg.build_asset_context(partition_key="2026-09-09") as context:
        assets.postprocess_forecasts(
            context,
            asset_config.OutputConfig(
                output_basedir="custom-output",
            ),
        )

    daily_output = Path("custom-output/2026-09-09_forecasts")
    postprocess.assert_called_once_with(
        base_forecast_dir=daily_output,
        forecast_date=dt.date(2026, 9, 9),
        execution_dir=daily_output / "run",
    )


@pytest.mark.parametrize(
    ("override_fields", "expected_lookbacks"),
    [
        pytest.param(
            {},
            (100, 120, 200),
            id="omitted-retains-model-defaults",
        ),
        pytest.param(
            {"n_lookback_days": 101},
            (101, 101, 101),
            id="value-applies-to-all-models",
        ),
        pytest.param(
            {"n_lookback_days": None},
            (None, None, None),
            id="explicit-null-applies-to-all-models",
        ),
    ],
)
def test_model_runners_use_per_model_lookbacks_and_location_overrides(
    monkeypatch,
    override_fields: dict[str, object],
    expected_lookbacks: tuple[int | None, int | None, int | None],
):
    monkeypatch.setattr(
        asset_helpers, "dagster_execution_directory", lambda context, base: base / "run"
    )
    context = SimpleNamespace(
        partition_key="2026-09-09",
        log=Mock(),
    )
    model_base_config = _model_base_config(override_fields)
    output_config = asset_config.OutputConfig(output_basedir="default-output")
    forecast_fable = Mock()
    forecast_pyrenew = Mock()
    forecast_nowcastautogp = Mock()

    monkeypatch.setattr(asset_helpers, "_throw_if_backfill", Mock())
    monkeypatch.setattr(asset_helpers, "forecast_fable", forecast_fable)
    monkeypatch.setattr(asset_helpers, "forecast_pyrenew", forecast_pyrenew)
    monkeypatch.setattr(asset_helpers, "forecast_nowcastautogp", forecast_nowcastautogp)

    asset_helpers._run_fable_e_other(
        context,
        asset_config.FableEOtherConfig(n_lookback_days=100),
        model_base_config,
        output_config,
        ed_visit_input_resolution="daily",
    )
    asset_helpers._run_pyrenew_model(
        context,
        asset_config.PyrenewConfig(n_lookback_days=120),
        model_base_config,
        output_config,
        model_letters="e",
    )
    asset_helpers._run_nowcastautogp_e_pct_epiweekly(
        context,
        asset_config.NowcastAutoGPEPctEpiweeklyConfig(n_lookback_days=200),
        model_base_config,
        output_config,
    )

    shared_arguments = {
        "output_dir": Path("default-output/2026-09-09_forecasts/run"),
        "exclude_last_n_days": 2,
        "fail_on_stale_data": True,
    }
    for forecast, expected_lookback in zip(
        (forecast_fable, forecast_pyrenew, forecast_nowcastautogp),
        expected_lookbacks,
        strict=True,
    ):
        for name, expected in shared_arguments.items():
            assert forecast.call_args.kwargs[name] == expected
        assert forecast.call_args.kwargs["n_lookback_days"] == expected_lookback


@pytest.mark.parametrize(
    "name,numerator,other,weekly",
    [
        ("fuse_pyrenew_e_fable", "pyrenew_e", "fable_e_other", False),
        (
            "fuse_pyrenew_e_fable_epiweekly",
            "pyrenew_e",
            "epiweekly_fable_e_other",
            True,
        ),
        ("fuse_pyrenew_he_fable", "pyrenew_he", "fable_e_other", False),
        (
            "fuse_pyrenew_he_fable_epiweekly",
            "pyrenew_he",
            "epiweekly_fable_e_other",
            True,
        ),
    ],
)
@pytest.mark.parametrize("pairs_present", [False, True])
def test_fusion_assets_discover_pairs_without_model_configuration(
    tmp_path, monkeypatch, name, numerator, other, weekly, pairs_present
):
    from cfa.stf.routine.fusion import FUSION_SPECS

    asset = getattr(assets, name)
    key = dg.AssetKey(name)
    assert asset.asset_deps[key] == {dg.AssetKey(numerator), dg.AssetKey(other)}
    assert asset.group_names_by_key[key] == "Fusion"
    assert isinstance(asset.node_def, dg.OpDefinition)
    assert "model_base_config" not in asset.required_resource_keys
    assert "output_config" in asset.required_resource_keys
    assert (
        key
        in assets.postprocess_forecasts.asset_deps[dg.AssetKey("postprocess_forecasts")]
    )
    spec = next(
        spec
        for spec in FUSION_SPECS
        if spec.numerator == numerator and spec.aggregate_num == weekly
    )
    partition = asset_config.daily_partitions_def.get_last_partition_key()
    base = tmp_path / f"{partition}_forecasts"
    older = base / "2026-09-09-08-00-00_first"
    newer = base / "2026-09-09-09-00-00_second"
    entries = [
        (older, FitKey("CA", "rsv", spec.numerator)),
        (older, FitKey("US", "covid", spec.other)),
    ]
    pairs = [("CA", "covid"), ("US", "flu")] if pairs_present else []
    for location, disease in pairs:
        entries.extend((older, source) for source in spec.input_keys(location, disease))
    if pairs_present:
        entries.append((newer, FitKey("CA", "covid", spec.other)))
    snapshot = {}
    for execution, source in entries:
        path = source.directory(execution)
        path.mkdir(parents=True, exist_ok=True)
        (path / "_COMPLETE").touch()
        snapshot[source] = path
    # An unfinished newer input must not displace a completed source.
    unfinished = FitKey("CA", "covid", spec.numerator).directory(newer)
    unfinished.mkdir(parents=True, exist_ok=True)
    runner = Mock()
    monkeypatch.setattr(asset_helpers, "_throw_if_backfill", Mock())
    monkeypatch.setattr(
        asset_helpers,
        "dagster_execution_directory",
        lambda ctx, base: base / "execution",
    )
    monkeypatch.setattr(asset_helpers, "materialize_fusion", runner)
    result = dg.materialize(
        [asset, dg.AssetSpec(numerator), dg.AssetSpec(other)],
        partition_key=partition,
        resources={"output_config": asset_config.OutputConfig()},
        run_config={
            "resources": {
                "output_config": {"config": {"output_basedir": str(tmp_path)}}
            }
        },
    )
    assert result.success
    assert runner.call_args_list == [
        call(spec, snapshot, base / "execution", location, disease)
        for location, disease in pairs
    ]
