"""Sequential fusion and summaries from a snapshot of completed fits."""

import argparse
import datetime as dt
import json
import logging
from pathlib import Path

import polars as pl

from cfa.stf.routine.run_storage import latest_fits, standalone_directory, write_json
from cfa.stf.routine.utils.collate_plots import merge_pdfs
from cfa.stf.routine.utils.prop_utils import create_prop_fusion_model
from cfa.stf.routine.utils.r_utils import (
    make_figures_from_model_fit_dir,
    model_fit_dir_to_hub_tbl,
)


def _hubverse_table_filename(forecast_date, disease):
    return f"{forecast_date}-{disease}-hubverse-table.parquet"


def combine_hubverse_tables(fits: list[Path], output: Path, disease: str) -> None:
    """Collate only the explicitly selected fits."""
    if not fits:
        raise FileNotFoundError("No hubverse_table.parquet files selected")
    combined = pl.concat(
        [pl.read_parquet(path / "hubverse_table.parquet") for path in fits],
        how="vertical_relaxed",
    )
    origins = combined["reference_date"].unique()
    if len(origins) != 1:
        raise ValueError("Expected one reference_date across selected fits")
    output.mkdir(parents=True, exist_ok=True)
    combined.write_parquet(output / _hubverse_table_filename(origins.item(), disease))


def main(
    base_forecast_dir: Path | str,
    diseases=("covid", "flu", "rsv"),
    *,
    execution_dir: Path | None = None,
) -> None:
    base = Path(base_forecast_dir)
    destination = execution_dir or standalone_directory(
        base, {"diseases": list(diseases)}
    )
    selected = {
        key: path
        for key, path in latest_fits(base).items()
        if key[1] in diseases and not key[2].startswith("prop_")
    }
    source_snapshot = {"/".join(key): str(path) for key, path in selected.items()}
    write_json(destination / "sources.json", source_snapshot)
    for location, disease in sorted({key[:2] for key in selected}):
        for numerator in ("pyrenew_e", "pyrenew_he"):
            for weekly in (False, True):
                other = "epiweekly_fable_e_other" if weekly else "daily_fable_e_other"
                num_dir = selected.get((location, disease, numerator))
                other_dir = selected.get((location, disease, other))
                if num_dir is None or other_dir is None:
                    logging.info(
                        "Skipping fusion %s/%s: missing %s or %s",
                        location,
                        disease,
                        numerator,
                        other,
                    )
                    continue
                metadata = json.loads((num_dir / "metadata.json").read_text())
                other_metadata = json.loads((other_dir / "metadata.json").read_text())
                if any(
                    metadata[k] != other_metadata[k]
                    for k in ("forecast_date", "forecast_through")
                ):
                    raise ValueError("Fusion input forecast windows differ")
                name = f"prop_{'epiweekly_aggregated_' if weekly else ''}{numerator}_{other}"
                fit = destination / location / disease / name
                fit.mkdir(parents=True, exist_ok=True)
                (fit / "complete.json").unlink(missing_ok=True)
                origin = dt.date.fromisoformat(metadata["forecast_date"])
                create_prop_fusion_model(
                    num_model_name=numerator,
                    other_model_name=other,
                    numerator_dir=num_dir,
                    other_dir=other_dir,
                    destination_dir=fit,
                    aggregate_num=weekly,
                    forecast_date=origin,
                    forecast_through=dt.date.fromisoformat(
                        metadata["forecast_through"]
                    ),
                )
                write_json(
                    fit / "metadata.json",
                    {
                        "model": name,
                        "location": location,
                        "disease": disease,
                        "forecast_date": metadata["forecast_date"],
                        "forecast_through": metadata["forecast_through"],
                        "sources": [str(num_dir), str(other_dir)],
                    },
                )
                make_figures_from_model_fit_dir(fit, save_figs=True, save_ci=True)
                model_fit_dir_to_hub_tbl(fit, forecast_date=origin)
                write_json(fit / "complete.json", {"complete": True})
                selected[(location, disease, name)] = fit
    for disease in diseases:
        fits = [path for key, path in sorted(selected.items()) if key[1] == disease]
        if not fits:
            continue
        output = destination / "summary" / disease
        output.mkdir(parents=True, exist_ok=True)
        combine_hubverse_tables(fits, output, disease)
        write_json(output / "sources.json", {"fits": [str(path) for path in fits]})
        pdfs = {}
        for fit in fits:
            for pdf in sorted((fit / "figures").glob("*.pdf")):
                pdfs.setdefault(pdf.name, []).append(pdf)
        for name, paths in pdfs.items():
            figure = output / "figures" / name
            figure.parent.mkdir(exist_ok=True)
            merge_pdfs(paths, figure)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("base_forecast_dir", type=Path)
    parser.add_argument("--diseases", nargs="+", default=["covid", "flu", "rsv"])
    main(**vars(parser.parse_args()))
