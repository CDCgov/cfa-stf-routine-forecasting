import datetime as dt
from enum import StrEnum
from zoneinfo import ZoneInfo

import dagster as dg
from cfa.stf.forecasttools import LOCATION_LIST
from cfa_dagster import GraphDimension, GraphDimensionExclusion
from cfa_dagster import is_production as is_prod
from pydantic import BaseModel, Field

# ============================================================================
# GRAPH DIMENSIONS AND PARTITIONS
# How are the data split and processed in Azure Batch?
# ============================================================================

DEFAULT_EXCLUDED_LOCATIONS = ["AS", "GU", "MP", "PR", "UM", "VI"]
SUPPORTED_DISEASES = ["covid", "flu", "rsv"]

# Disease dimensions
DISEASES = SUPPORTED_DISEASES
Disease = StrEnum("Disease", {v: v for v in DISEASES})

# Location dimensions
LOCATIONS = [
    location for location in LOCATION_LIST if location not in DEFAULT_EXCLUDED_LOCATIONS
]
Location = StrEnum("Location", {v: v for v in LOCATIONS})

# Daily Partitions
tz = "America/New_York"
daily_partitions_def = dg.DailyPartitionsDefinition(
    start_date=dt.datetime.now(ZoneInfo(tz)) - dt.timedelta(days=1),
    end_offset=1,
    timezone=tz,
)

# ============================================================================
# ASSET CONFIGURATIONS
# ============================================================================


# Use default_factory to prevent ConfigOverrides from populating fields in the
# Launchpad unless the user explicitly sets them.
class _SharedModelConfigFields(BaseModel):
    output_basedir: str = Field(
        default_factory=lambda: "",
        description="Output directory used by all forecast models.",
    )
    exclude_last_n_days: int = Field(
        default_factory=lambda: 0,
        description="Requested recent-data omission used by all forecast models.",
    )
    fail_on_stale_data: bool = Field(
        default_factory=lambda: is_prod(),
        description="Stale-input policy used by all forecast models.",
    )


class ConfigOverride(_SharedModelConfigFields, dg.Config):
    location: Location  # type: ignore[reportInvalidTypeForm]
    n_lookback_days: int | None = Field(
        default_factory=lambda: None,
        description=(
            "Training lookback applied to all forecast models for this location."
        ),
    )

    def as_dict(self) -> dict:  # type: ignore[reportInvalidTypeForm]
        return self.model_dump(mode="json", exclude_unset=True)


class ModelBaseConfig(_SharedModelConfigFields, dg.ConfigurableResource):
    """
    Shared configuration and explicitly model-scoped lookbacks for model assets.
    """

    output_basedir: str = Field(
        default="output" if is_prod() else "test-output",
        description="Output directory used by all forecast models.",
    )
    fable_pyrenew_n_lookback_days: int | None = Field(
        default=150,
        description="Training lookback used only by Fable and PyRenew models.",
    )
    epiautogp_n_lookback_days: int | None = Field(
        default=None if is_prod() else 150,
        description="Training lookback used only by EpiAutoGP models.",
    )
    exclude_last_n_days: int = Field(
        default=1,
        description="Requested recent-data omission used by all forecast models.",
    )
    fail_on_stale_data: bool = Field(
        default=is_prod(),
        description="Stale-input policy used by all forecast models.",
    )
    diseases: GraphDimension[Disease] = Field(  # type: ignore[reportInvalidTypeForm]
        default=GraphDimension(DISEASES),
        description="Diseases run by all selected forecast models.",
    )
    locations: GraphDimension[Location] = Field(  # type: ignore[reportInvalidTypeForm]
        default=GraphDimension(LOCATIONS),
        description="Locations run by all selected forecast models.",
    )
    # Add defaults here, or add in the launchpad with ctrl+space
    config_overrides: list[ConfigOverride] = Field(
        default=[
            # ConfigOverride(location="GA", exclude_last_n_days=2).as_dict(),
        ],
        description=(
            "Provide location-specific overrides as a list of dicts. "
            "An explicitly provided n_lookback_days applies to all models, "
            "which otherwise retain their model-specific defaults. "
            "The Launchpad accepts both YAML and JSON-style lists, e.g. "
            "config_overrides: [{ location: GA, n_lookback_days: 120 }]."
        ),
    )  # type: ignore[reportInvalidTypeForm]

    def get_by_location(self, loc: Location) -> "ModelBaseConfig":  # type: ignore[reportInvalidTypeForm]
        overrides = {}
        for entry in self.config_overrides:
            if isinstance(entry, dict):
                entry = ConfigOverride(**entry)
            if entry.location == loc:
                overrides = entry.model_dump(exclude={"location"}, exclude_unset=True)
                if "n_lookback_days" in overrides:
                    n_lookback_days = overrides.pop("n_lookback_days")
                    overrides["fable_pyrenew_n_lookback_days"] = n_lookback_days
                    overrides["epiautogp_n_lookback_days"] = n_lookback_days
                break
        return self.model_copy(update=overrides)


class FableEOtherConfig(dg.ConfigurableResource):
    """
    Configuration for fable E-other model assets
    (fable_e_other, epiweekly_fable_e_other).
    These default values can be modified in the Dagster asset materialization launchpad.
    """

    n_samples: int = 400 if not is_prod() else 2000


class PyrenewConfig(dg.ConfigurableResource):
    """
    Configuration for Pyrenew model assets (pyrenew_e, pyrenew_h, pyrenew_he, etc.).
    These default values can be modified in the Dagster asset materialization launchpad.
    """

    n_warmup: int = 200 if not is_prod() else 1000
    n_samples: int = 200 if not is_prod() else 500
    n_chains: int = 2 if not is_prod() else 4
    rng_key: int = 12345
    additional_forecast_letters: str = ""


class EpiAutoGPEPctEpiweeklyConfig(dg.ConfigurableResource):
    """Configuration for the epiweekly EpiAutoGP E-pct model asset."""

    n_particles: int = 64 if is_prod() else 4
    n_mcmc: int = 200 if is_prod() else 100
    n_hmc: int = 50 if is_prod() else 25
    n_forecast_draws: int = 2000
    smc_data_proportion: float = 0.1
    n_threads: str = "auto"


class EModelExclusions(dg.ConfigurableResource):
    # filter out WY
    locations: GraphDimensionExclusion[Location] = GraphDimensionExclusion(["WY"])  # type: ignore[reportInvalidTypeForm]


class WModelExclusions(dg.ConfigurableResource):
    # only covid is valid for W
    diseases: GraphDimension[Disease] = GraphDimension(["covid"])  # type: ignore[reportInvalidTypeForm]


class PostProcessConfig(dg.Config):
    """
    Configuration for the Post-Processing asset.
    """

    output_basedir: str = "output" if is_prod() else "test-output"
    skip_existing: bool = False
    postprocess_diseases: list[str] = ["covid", "flu", "rsv"]
