import pickle
from pathlib import Path

import jax
import numpy as np
from pyrenew_multisignal.hew import PyrenewHEWData

from cfa.stf.routine.pyrenew_hew.utils import build_pyrenew_hew_model_from_dir


def fit_and_save_model(
    model_dir: str,
    fit_ed_visits: bool = False,
    fit_hospital_admissions: bool = False,
    fit_wastewater: bool = False,
    n_warmup: int = 1000,
    n_samples: int = 1000,
    n_chains: int = 4,
    rng_key: int | None = None,
) -> None:
    if rng_key is None:
        rng_key = np.random.randint(0, 10000)
    if isinstance(rng_key, int):
        rng_key = jax.random.key(rng_key)
    else:
        raise ValueError(
            "rng_key must be an integer with which to seed `jax.random.key`"
        )

    my_data = PyrenewHEWData.from_json(
        json_file_path=Path(model_dir) / "data" / "data_for_model_fit.json",
        fit_ed_visits=fit_ed_visits,
        fit_hospital_admissions=fit_hospital_admissions,
        fit_wastewater=fit_wastewater,
    )
    my_model = build_pyrenew_hew_model_from_dir(
        model_dir,
        fit_ed_visits=fit_ed_visits,
        fit_hospital_admissions=fit_hospital_admissions,
        fit_wastewater=fit_wastewater,
    )
    my_model.run(
        data=my_data,
        sample_ed_visits=fit_ed_visits,
        sample_hospital_admissions=fit_hospital_admissions,
        sample_wastewater=fit_wastewater,
        num_warmup=n_warmup,
        num_samples=n_samples,
        rng_key=rng_key,
        extra_fields=(
            "potential_energy",
            "num_steps",
            "z_grad",
            "accept_prob",
        ),
        mcmc_args=dict(num_chains=n_chains, progress_bar=True),
        nuts_args=dict(find_heuristic_step_size=True),
    )

    my_model.mcmc.sampler = None
    with open(
        model_dir / "posterior_samples.pickle",
        "wb",
    ) as file:
        pickle.dump(my_model.mcmc, file)
