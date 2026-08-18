"""Resolve and serialize model-specific inputs for PyRenew HEW."""

import json
from dataclasses import dataclass
from pathlib import Path

import jax.numpy as jnp
from cfa.stf.data import (
    get_nnh_delay_pmf,
    get_nnh_generation_interval_pmf,
    get_nnh_right_truncation_pmf,
)
from pyrenew_multisignal.hew import approx_lognorm

from cfa.stf.routine.forecast_run import ForecastRun


@dataclass(frozen=True)
class PyRenewModelInputs:
    """Fixed, run-vintaged model inputs required by PyRenew HEW."""

    generation_interval_pmf: tuple[float, ...]
    infection_to_admission_pmf: tuple[float, ...]
    right_truncation_pmf: tuple[float, ...]


def resolve_pyrenew_model_inputs(
    *,
    run: ForecastRun,
    fit_ed_visits: bool,
) -> PyRenewModelInputs:
    """Load NNH PMFs for the location, disease, and vintage of a forecast run."""
    generation_interval_pmf = tuple(
        get_nnh_generation_interval_pmf(
            disease=run.disease,
            as_of=run.report_date,
        )
    )
    raw_delay_pmf = list(
        get_nnh_delay_pmf(
            disease=run.disease,
            as_of=run.report_date,
        )
    )
    # We do not model a zero infection-to-recorded-admission delay.
    raw_delay_pmf[0] = 0.0
    normalized_delay = jnp.array(raw_delay_pmf)
    infection_to_admission_pmf = tuple(
        (normalized_delay / normalized_delay.sum()).tolist()
    )

    try:
        right_truncation_pmf = tuple(
            get_nnh_right_truncation_pmf(
                state_abb=run.loc,
                disease=run.disease,
                as_of=run.report_date,
                reference_date=run.report_date,
            )
        )
    except ValueError:
        if fit_ed_visits:
            raise
        right_truncation_pmf = (1.0,)

    return PyRenewModelInputs(
        generation_interval_pmf=generation_interval_pmf,
        infection_to_admission_pmf=infection_to_admission_pmf,
        right_truncation_pmf=right_truncation_pmf,
    )


def serialize_pyrenew_model_params(
    *,
    run: ForecastRun,
    model_inputs: PyRenewModelInputs,
    save_dir: Path,
) -> None:
    """Serialize resolved PyRenew model inputs and their derived parameters."""
    lognormal_loc, lognormal_scale = approx_lognorm(
        jnp.array(model_inputs.infection_to_admission_pmf)[1:],
        loc_guess=0,
        scale_guess=0.5,
    )
    model_params = {
        "population_size": run.loc_pop,
        "pop_fraction": [1],
        "generation_interval_pmf": model_inputs.generation_interval_pmf,
        "right_truncation_pmf": model_inputs.right_truncation_pmf,
        "inf_to_hosp_admit_lognormal_loc": lognormal_loc,
        "inf_to_hosp_admit_lognormal_scale": lognormal_scale,
        "inf_to_hosp_admit_pmf": model_inputs.infection_to_admission_pmf,
    }
    with open(Path(save_dir, "model_params.json"), "w") as json_file:
        json.dump(model_params, json_file, default=str)
