"""R-backed forecast postprocessing utilities."""

from pathlib import Path

from cfa.stf.routine._paths import UTILS_DIR
from cfa.stf.routine.utils.language_utils import run_r_script


def make_figures_from_model_fit_dir(
    model_fit_dir: Path,
    save_ci: bool = False,
    save_figs: bool = True,
) -> None:
    """Generate forecast figures from a model fit directory."""
    args = [f"{model_fit_dir}"]
    if save_ci:
        args.append("--save-ci")
    if save_figs:
        args.append("--save-figs")

    run_r_script(
        UTILS_DIR / "make_figures_from_model_fit_dir.R",
        args,
        function_name="make_figures_from_model_fit_dir",
    )


def py_scalar_to_r_scalar(py_scalar):
    """Convert a Python scalar to an R scalar literal."""
    if py_scalar is None:
        return "NULL"
    return f"'{str(py_scalar)}'"


def model_fit_dir_to_hub_tbl(
    model_fit_dir: Path | str,
    output_type: str = "samples",
) -> None:
    """Create a hubverse table from a model fit directory."""
    run_r_script(
        UTILS_DIR / "model_fit_dir_to_hub_tbl.R",
        [str(model_fit_dir), "--output-type", output_type],
        function_name="model_fit_dir_to_hub_tbl",
    )
