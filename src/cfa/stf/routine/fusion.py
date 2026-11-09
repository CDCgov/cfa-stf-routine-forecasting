"""Fusion recipes, materialization, and provenance validation."""

import datetime as dt
import json
from dataclasses import dataclass
from pathlib import Path

from cfa.stf.routine.run_storage import FitKey, write_json
from cfa.stf.routine.utils.prop_utils import create_prop_fusion_model
from cfa.stf.routine.utils.r_utils import (
    make_figures_from_model_fit_dir,
    model_fit_dir_to_hub_tbl,
)


@dataclass(frozen=True)
class FusionSpec:
    """Input models and aggregation settings shared by fusion and provenance checks."""

    numerator: str
    other: str
    aggregate_num: bool = False
    aggregate_other: bool = False

    @property
    def model_name(self) -> str:
        numerator = (
            f"epiweekly_aggregated_{self.numerator}"
            if self.aggregate_num
            else self.numerator
        )
        other = (
            f"epiweekly_aggregated_{self.other}" if self.aggregate_other else self.other
        )
        return f"prop_{numerator}_{other}"

    def input_keys(self, location: str, disease: str) -> tuple[FitKey, FitKey]:
        return (
            FitKey(location, disease, self.numerator),
            FitKey(location, disease, self.other),
        )


PYRENEW_E_FABLE = FusionSpec("pyrenew_e", "daily_fable_e_other")
PYRENEW_E_FABLE_EPIWEEKLY = FusionSpec(
    "pyrenew_e", "epiweekly_fable_e_other", aggregate_num=True
)
PYRENEW_HE_FABLE = FusionSpec("pyrenew_he", "daily_fable_e_other")
PYRENEW_HE_FABLE_EPIWEEKLY = FusionSpec(
    "pyrenew_he", "epiweekly_fable_e_other", aggregate_num=True
)

FUSION_SPECS = (
    PYRENEW_E_FABLE,
    PYRENEW_E_FABLE_EPIWEEKLY,
    PYRENEW_HE_FABLE,
    PYRENEW_HE_FABLE_EPIWEEKLY,
)


def run_fusion(
    spec: FusionSpec,
    inputs: dict[FitKey, Path],
    execution: Path,
    location: str,
    disease: str,
) -> Path:
    """Write one completed fusion under the executing run's directory."""
    keys = spec.input_keys(location, disease)
    missing = [key for key in keys if key not in inputs]
    if missing:
        raise FileNotFoundError(f"Missing completed fusion inputs: {missing}")
    numerator, other = (inputs[key] for key in keys)
    metadata, other_metadata = (
        json.loads((path / "metadata.json").read_text()) for path in (numerator, other)
    )
    if any(
        metadata[k] != other_metadata[k] for k in ("forecast_date", "forecast_through")
    ):
        raise ValueError("Fusion input forecast windows differ")
    origin = dt.date.fromisoformat(metadata["forecast_date"])
    destination = FitKey(location, disease, spec.model_name).directory(execution)
    destination.mkdir(parents=True, exist_ok=True)
    (destination / "_COMPLETE").unlink(missing_ok=True)
    create_prop_fusion_model(
        numerator_dir=numerator,
        other_dir=other,
        destination_dir=destination,
        aggregate_num=spec.aggregate_num,
        aggregate_other=spec.aggregate_other,
        forecast_date=origin,
        forecast_through=dt.date.fromisoformat(metadata["forecast_through"]),
    )
    write_json(
        destination / "metadata.json",
        {
            "model": spec.model_name,
            "location": location,
            "disease": disease,
            "forecast_date": metadata["forecast_date"],
            "forecast_through": metadata["forecast_through"],
            "sources": [str(numerator), str(other)],
        },
    )
    make_figures_from_model_fit_dir(destination, save_figs=True, save_ci=True)
    model_fit_dir_to_hub_tbl(destination, forecast_date=origin)
    (destination / "_COMPLETE").touch()
    return destination


def validate_fusion_sources(snapshot: dict[FitKey, Path]) -> None:
    """Raise if a fusion's recorded inputs differ from the selected completed fits."""
    specs = {spec.model_name: spec for spec in FUSION_SPECS}
    for key, path in snapshot.items():
        if not key.model.startswith("prop_"):
            continue
        spec = specs.get(key.model)
        if spec is None:
            raise ValueError(f"Unknown fusion recipe {key.model!r} at {path}")
        expected = [
            str(snapshot[source].resolve()) if source in snapshot else None
            for source in spec.input_keys(key.location, key.disease)
        ]
        metadata = json.loads((path / "metadata.json").read_text())
        sources = metadata.get("sources", [])
        if (
            None in expected
            or len(sources) != 2
            or [str(Path(source).resolve()) for source in sources] != expected
        ):
            raise ValueError(
                f"Stale fusion at {path}: recorded sources {sources!r} do not match "
                f"the latest completed inputs {expected!r}. "
                "Rerun the fusion before postprocessing."
            )
