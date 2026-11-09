"Internal helper functions called by our dagster assets."

import datetime as dt
from pathlib import Path

import dagster as dg
from pyrenew_multisignal.hew.utils import flags_from_hew_letters

from cfa.stf.routine._paths import PRODUCTION_PRIORS
from cfa.stf.routine.dagster.defs.asset_config import (
    FableEOtherConfig,
    FusionConfig,
    ModelBaseConfig,
    NowcastAutoGPEPctEpiweeklyConfig,
    PyrenewConfig,
    daily_partitions_def,
)
from cfa.stf.routine.data.data_access import DataResolution
from cfa.stf.routine.fable.forecast_fable import main as forecast_fable
from cfa.stf.routine.fusion import FusionSpec, materialize_fusion
from cfa.stf.routine.nowcastautogp.forecast_nowcastautogp import (
    main as forecast_nowcastautogp,
)
from cfa.stf.routine.pyrenew_hew.forecast_pyrenew import main as forecast_pyrenew
from cfa.stf.routine.run_storage import dagster_execution_directory, find_fits

# Utility Functions


def _throw_if_backfill(
    context: dg.OpExecutionContext | dg.AssetExecutionContext,
    partition_def: dg.PartitionsDefinition,
):
    current_partition = context.partition_key
    latest_partition = partition_def.get_last_partition_key()
    if current_partition != latest_partition:
        raise RuntimeError("STF forecast models do not support backfills")


# Model Runner functions


def _run_fable_e_other(
    context: dg.OpExecutionContext,
    fable_e_other_config: FableEOtherConfig,
    model_base_config: ModelBaseConfig,
    ed_visit_input_resolution: DataResolution,
) -> str | None:
    """Run a Fable E-other model at the requested ED-visit resolution."""
    _throw_if_backfill(context, daily_partitions_def)

    disease = model_base_config.diseases.current_value
    location = model_base_config.locations.current_value
    forecast_date = dt.datetime.strptime(context.partition_key, "%Y-%m-%d").date()

    loc_config = model_base_config.get_by_location(location)
    context.log.debug(f"loc_config: '{loc_config}'")

    # We let the user potentially override the basedir, but the subdirectory is
    # locked to the partition date.
    daily_forecast_output_dir: Path = Path(
        model_base_config.output_basedir,
        f"{context.partition_key}_forecasts",
    )

    context.log.info(f"fable_e_other_config: '{fable_e_other_config}'")
    daily_forecast_output_dir = dagster_execution_directory(
        context, daily_forecast_output_dir
    )
    context.log.info(f"Will write to: {daily_forecast_output_dir}")
    forecast_fable(
        disease=disease,
        loc=location,
        output_dir=daily_forecast_output_dir,
        n_lookback_days=loc_config.fable_pyrenew_n_lookback_days,
        n_samples=fable_e_other_config.n_samples,
        exclude_last_n_days=loc_config.exclude_last_n_days,
        ed_visit_input_resolution=ed_visit_input_resolution,
        forecast_date=forecast_date,
        fail_on_stale_data=loc_config.fail_on_stale_data,
    )


def _run_pyrenew_model(
    context: dg.OpExecutionContext,
    pyrenew_config: PyrenewConfig,
    model_base_config: ModelBaseConfig,
    model_letters: str,
) -> str | None:
    """
    Helper to run Pyrenew models with common arguments.
    """
    _throw_if_backfill(context, daily_partitions_def)

    disease = model_base_config.diseases.current_value
    location = model_base_config.locations.current_value
    forecast_date = dt.datetime.strptime(context.partition_key, "%Y-%m-%d").date()

    loc_config = model_base_config.get_by_location(location)
    context.log.debug(f"loc_config: '{loc_config}'")

    # We let the user potentially override the basedir, but the subdirectory is
    # locked to the partition date.
    daily_forecast_output_dir: Path = Path(
        model_base_config.output_basedir, f"{context.partition_key}_forecasts"
    )

    fit_flags = flags_from_hew_letters(model_letters)
    forecast_flags = flags_from_hew_letters(
        f"{model_letters}{pyrenew_config.additional_forecast_letters}",
        flag_prefix="forecast",
    )
    context.log.info(f"config: '{pyrenew_config}'")
    daily_forecast_output_dir = dagster_execution_directory(
        context, daily_forecast_output_dir
    )
    context.log.info(f"Will write to: {daily_forecast_output_dir}")
    forecast_pyrenew(
        disease=disease,
        loc=location,
        priors_path=PRODUCTION_PRIORS,
        output_dir=daily_forecast_output_dir,
        n_lookback_days=loc_config.fable_pyrenew_n_lookback_days,
        n_chains=pyrenew_config.n_chains,
        n_warmup=pyrenew_config.n_warmup,
        n_samples=pyrenew_config.n_samples,
        exclude_last_n_days=loc_config.exclude_last_n_days,
        rng_key=pyrenew_config.rng_key,
        forecast_date=forecast_date,
        fail_on_stale_data=loc_config.fail_on_stale_data,
        **fit_flags,
        **forecast_flags,
    )


def _run_nowcastautogp_e_pct_epiweekly(
    context: dg.OpExecutionContext,
    nowcastautogp_e_pct_epiweekly_config: NowcastAutoGPEPctEpiweeklyConfig,
    model_base_config: ModelBaseConfig,
) -> None:
    """Run NowcastAutoGP directly on epiweekly NSSP percentage data."""
    _throw_if_backfill(context, daily_partitions_def)

    disease = model_base_config.diseases.current_value
    location = model_base_config.locations.current_value
    forecast_date = dt.datetime.strptime(context.partition_key, "%Y-%m-%d").date()
    loc_config = model_base_config.get_by_location(location)
    context.log.debug(f"loc_config: '{loc_config}'")
    daily_forecast_output_dir = Path(
        model_base_config.output_basedir,
        f"{context.partition_key}_forecasts",
    )
    context.log.info(
        f"nowcastautogp_e_pct_epiweekly_config: '{nowcastautogp_e_pct_epiweekly_config}'"
    )
    daily_forecast_output_dir = dagster_execution_directory(
        context, daily_forecast_output_dir
    )
    context.log.info(f"Will write to: {daily_forecast_output_dir}")

    forecast_nowcastautogp(
        disease=disease,
        loc=location,
        output_dir=daily_forecast_output_dir,
        n_lookback_days=loc_config.nowcastautogp_n_lookback_days,
        target="nssp",
        frequency="epiweekly",
        ed_visit_type="pct",
        exclude_last_n_days=loc_config.exclude_last_n_days,
        n_particles=nowcastautogp_e_pct_epiweekly_config.n_particles,
        n_mcmc=nowcastautogp_e_pct_epiweekly_config.n_mcmc,
        n_hmc=nowcastautogp_e_pct_epiweekly_config.n_hmc,
        n_forecast_draws=nowcastautogp_e_pct_epiweekly_config.n_forecast_draws,
        smc_data_proportion=nowcastautogp_e_pct_epiweekly_config.smc_data_proportion,
        n_threads=nowcastautogp_e_pct_epiweekly_config.n_threads,
        nowcast_source_name="none",
        forecast_date=forecast_date,
        fail_on_stale_data=loc_config.fail_on_stale_data,
        logger=context.log,
    )


def _run_fusion_model(
    context: dg.AssetExecutionContext,
    config: FusionConfig,
    spec: FusionSpec,
) -> list[Path]:
    """Run a fusion recipe for every location/disease pair with completed inputs."""
    _throw_if_backfill(context, daily_partitions_def)
    base = Path(config.output_basedir, f"{context.partition_key}_forecasts")
    execution = dagster_execution_directory(context, base)
    inputs = find_fits(base)

    def pairs_for(model: str) -> set[tuple[str, str]]:
        return {(key.location, key.disease) for key in inputs if key.model == model}

    pairs = sorted(pairs_for(spec.numerator) & pairs_for(spec.other))
    context.log.info(
        "Fusing %d completed input pairs for %s", len(pairs), spec.model_name
    )
    return [
        materialize_fusion(spec, inputs, execution, location, disease)
        for location, disease in pairs
    ]
