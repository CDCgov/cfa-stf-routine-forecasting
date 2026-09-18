import datetime as dt
from pathlib import Path

import dagster as dg
from pyrenew_multisignal.hew.utils import flags_from_hew_letters

from cfa.stf.routine._paths import PRODUCTION_PRIORS
from cfa.stf.routine.dagster.defs.asset_config import (
    EpiAutoGPEPctEpiweeklyConfig,
    FableEOtherConfig,
    ModelBaseConfig,
    PyrenewConfig,
    daily_partitions_def,
)
from cfa.stf.routine.data.data_access import DataResolution
from cfa.stf.routine.epiautogp.forecast_epiautogp import main as forecast_epiautogp
from cfa.stf.routine.fable.forecast_fable import main as forecast_fable
from cfa.stf.routine.forecast_window import ForecastWindow
from cfa.stf.routine.pyrenew_hew.forecast_pyrenew import main as forecast_pyrenew
from cfa.stf.routine.utils.prop_utils import create_prop_fusion_model
from cfa.stf.routine.utils.r_utils import (
    make_figures_from_model_fit_dir,
    model_fit_dir_to_hub_tbl,
)

# ============================================================================
# MODEL CONSTRUCTOR FUNCTIONS - these are used later, in Asset Definitions
# ============================================================================


def _throw_if_backfill(
    context: dg.OpExecutionContext | dg.AssetExecutionContext,
    partition_def: dg.PartitionsDefinition,
):
    current_partition = context.partition_key
    latest_partition = partition_def.get_last_partition_key()
    if current_partition != latest_partition:
        raise RuntimeError("STF forecast models do not support backfills")


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
    run_date = dt.datetime.strptime(context.partition_key, "%Y-%m-%d").date()

    loc_config = model_base_config.get_by_location(location)
    context.log.debug(f"loc_config: '{loc_config}'")

    # We let the user potentially override the basedir, but the subdirectory is
    # locked to the partition date.
    daily_forecast_output_dir: Path = Path(
        loc_config.output_basedir,
        f"{context.partition_key}_forecasts",
    )

    context.log.info(f"fable_e_other_config: '{fable_e_other_config}'")
    context.log.info(f"Will write to: {daily_forecast_output_dir}")
    forecast_fable(
        disease=disease,
        loc=location,
        output_dir=daily_forecast_output_dir,
        n_lookback_days=loc_config.fable_pyrenew_n_lookback_days,
        n_samples=fable_e_other_config.n_samples,
        exclude_last_n_days=loc_config.exclude_last_n_days,
        ed_visit_input_resolution=ed_visit_input_resolution,
        run_date=run_date,
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
    run_date = dt.datetime.strptime(context.partition_key, "%Y-%m-%d").date()

    loc_config = model_base_config.get_by_location(location)
    context.log.debug(f"loc_config: '{loc_config}'")

    # We let the user potentially override the basedir, but the subdirectory is
    # locked to the partition date.
    daily_forecast_output_dir: Path = Path(
        loc_config.output_basedir, f"{context.partition_key}_forecasts"
    )

    fit_flags = flags_from_hew_letters(model_letters)
    forecast_flags = flags_from_hew_letters(
        f"{model_letters}{pyrenew_config.additional_forecast_letters}",
        flag_prefix="forecast",
    )
    context.log.info(f"config: '{pyrenew_config}'")
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
        run_date=run_date,
        fail_on_stale_data=loc_config.fail_on_stale_data,
        **fit_flags,
        **forecast_flags,
    )


def _run_epiautogp_e_pct_epiweekly(
    context: dg.OpExecutionContext,
    epiautogp_e_pct_epiweekly_config: EpiAutoGPEPctEpiweeklyConfig,
    model_base_config: ModelBaseConfig,
) -> None:
    """Run EpiAutoGP directly on epiweekly NSSP percentage data."""
    _throw_if_backfill(context, daily_partitions_def)

    disease = model_base_config.diseases.current_value
    location = model_base_config.locations.current_value
    run_date = dt.datetime.strptime(context.partition_key, "%Y-%m-%d").date()
    loc_config = model_base_config.get_by_location(location)
    context.log.debug(f"loc_config: '{loc_config}'")
    daily_forecast_output_dir = Path(
        loc_config.output_basedir,
        f"{context.partition_key}_forecasts",
    )
    context.log.info(
        f"epiautogp_e_pct_epiweekly_config: '{epiautogp_e_pct_epiweekly_config}'"
    )
    context.log.info(f"Will write to: {daily_forecast_output_dir}")

    forecast_epiautogp(
        disease=disease,
        loc=location,
        output_dir=daily_forecast_output_dir,
        n_lookback_days=loc_config.epiautogp_n_lookback_days,
        target="nssp",
        frequency="epiweekly",
        ed_visit_type="pct",
        exclude_last_n_days=loc_config.exclude_last_n_days,
        n_particles=epiautogp_e_pct_epiweekly_config.n_particles,
        n_mcmc=epiautogp_e_pct_epiweekly_config.n_mcmc,
        n_hmc=epiautogp_e_pct_epiweekly_config.n_hmc,
        n_forecast_draws=epiautogp_e_pct_epiweekly_config.n_forecast_draws,
        smc_data_proportion=epiautogp_e_pct_epiweekly_config.smc_data_proportion,
        n_threads=epiautogp_e_pct_epiweekly_config.n_threads,
        nowcast_source_name="none",
        run_date=run_date,
        fail_on_stale_data=loc_config.fail_on_stale_data,
        logger=context.log,
    )


def _get_model_loc_dir(
    context: dg.OpExecutionContext,
    model_base_config: ModelBaseConfig,
) -> Path:
    disease = model_base_config.diseases.current_value
    location = model_base_config.locations.current_value

    loc_config = model_base_config.get_by_location(location)
    context.log.debug(f"loc_config: '{loc_config}'")

    run_date = dt.datetime.strptime(context.partition_key, "%Y-%m-%d").date()
    forecast_window = ForecastWindow(
        report_date=run_date,
        n_lookback_days=loc_config.fable_pyrenew_n_lookback_days,
        exclude_last_n_days=loc_config.exclude_last_n_days,
    )
    model_batch_dir_name = forecast_window.model_batch_dir_name(disease)

    model_loc_dir = Path(
        loc_config.output_basedir,
        f"{context.partition_key}_forecasts",
        model_batch_dir_name,
        "model_runs",
        location,
    )
    return model_loc_dir


def _run_fusion_model(
    context: dg.OpExecutionContext,
    model_base_config: ModelBaseConfig,
    num_model_name,
    other_model_name,
    aggregate_num,
    aggregate_other,
    fusion_model_name,
) -> str | None:
    """
    Helper function to run fusion model.
    """
    _throw_if_backfill(context, daily_partitions_def)
    model_loc_dir = _get_model_loc_dir(context, model_base_config)
    create_prop_fusion_model(
        model_run_dir=model_loc_dir,
        num_model_name=num_model_name,
        other_model_name=other_model_name,
        aggregate_num=aggregate_num,
        aggregate_other=aggregate_other,
    )

    fusion_model_fit_dir = Path(model_loc_dir, fusion_model_name)

    make_figures_from_model_fit_dir(fusion_model_fit_dir)

    make_figures_from_model_fit_dir(
        fusion_model_fit_dir,
        save_figs=True,
        save_ci=True,
    )
    run_date = dt.datetime.strptime(context.partition_key, "%Y-%m-%d").date()
    model_fit_dir_to_hub_tbl(fusion_model_fit_dir, report_date=run_date)

    context.log.debug(f"config: '{model_base_config}'")


def _fuse_pyrenew_fable_e_other(
    context,
    model_base_config: ModelBaseConfig,
    pyrenew_model_name,
    epiweekly: bool,
):
    other_model_name = "epiweekly_fable_e_other" if epiweekly else "daily_fable_e_other"
    fusion_model_name = (
        f"prop_epiweekly_aggregated_{pyrenew_model_name}_epiweekly_fable_e_other"
        if epiweekly
        else f"prop_{pyrenew_model_name}_daily_fable_e_other"
    )
    aggregate_num = epiweekly
    _run_fusion_model(
        context=context,
        model_base_config=model_base_config,
        num_model_name=pyrenew_model_name,
        other_model_name=other_model_name,
        aggregate_num=aggregate_num,
        aggregate_other=False,
        fusion_model_name=fusion_model_name,
    )
