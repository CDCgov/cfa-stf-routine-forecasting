"""Utilities for constructing PyRenew HEW models."""

import runpy
from pathlib import Path

from pyrenew_multisignal.hew import PyrenewHEWParam, build_pyrenew_hew_model


def build_pyrenew_hew_model_from_dir(
    model_dir: Path | str,
    fit_ed_visits: bool = False,
    fit_hospital_admissions: bool = False,
):
    """Build a PyRenew HEW model from its saved priors and parameters."""
    model_dir = Path(model_dir)
    priors = runpy.run_path(str(model_dir / "priors.py"))
    model_params = PyrenewHEWParam.from_json(model_dir / "data" / "model_params.json")
    return build_pyrenew_hew_model(
        priors,
        model_params,
        fit_ed_visits=fit_ed_visits,
        fit_hospital_admissions=fit_hospital_admissions,
    )
