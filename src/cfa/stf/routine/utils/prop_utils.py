"""Utilities for creating and appending disease proportion data."""

from pathlib import Path

from cfa.stf.forecasttools import append_prop_data, read_tabular, write_tabular

from cfa.stf.routine._paths import UTILS_DIR
from cfa.stf.routine.utils.language_utils import run_r_script


def create_prop_fusion_model(
    model_run_dir: Path | str,
    num_model_name: str,
    other_model_name: str,
    num_var_name: str = "observed_ed_visits",
    other_var_name: str = "other_ed_visits",
    prop_var_name: str = "prop_disease_ed_visits",
    augment_num_with_obs: bool = False,
    augment_other_with_obs: bool = True,
    aggregate_num: bool = False,
    aggregate_other: bool = False,
) -> None:
    """Create a proportion fusion model using the bundled R script."""
    args = [
        str(model_run_dir),
        "--num-model-name",
        num_model_name,
        "--other-model-name",
        other_model_name,
        "--num-var-name",
        num_var_name,
        "--other-var-name",
        other_var_name,
        "--prop-var-name",
        prop_var_name,
    ]
    if augment_num_with_obs:
        args.append("--augment-num-with-obs")
    if augment_other_with_obs:
        args.append("--augment-other-with-obs")
    if aggregate_num:
        args.append("--aggregate-num")
    if aggregate_other:
        args.append("--aggregate-other")
    args.append("--save")

    run_r_script(
        UTILS_DIR / "create_prop_fusion_model.R",
        args,
        function_name="create_prop_fusion_model",
    )


def append_prop_data_to_combined_data(
    data_path: Path | str,
    observed_var: str = "observed_ed_visits",
    other_var: str = "other_ed_visits",
    prop_var: str = "prop_disease_ed_visits",
) -> None:
    """Append disease ED visit proportion rows when both inputs are available."""
    path = Path(data_path)
    data = read_tabular(path)

    required_vars = {observed_var, other_var}
    available_vars = set(data.get_column(".variable").unique().to_list())
    present_required_vars = required_vars & available_vars
    if not present_required_vars:
        return
    if present_required_vars != required_vars:
        missing_vars = ", ".join(sorted(required_vars - available_vars))
        raise ValueError(
            "Cannot append ED visit proportions from incomplete NSSP data; "
            f"missing variable(s): {missing_vars}"
        )

    data = append_prop_data(
        data,
        observed_var=observed_var,
        other_var=other_var,
        prop_var=prop_var,
    )
    write_tabular(data, path)
