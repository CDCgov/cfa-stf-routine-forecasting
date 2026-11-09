"""Summaries from a snapshot of completed, current fits."""

import argparse
from pathlib import Path

import polars as pl

from cfa.stf.routine.fusion import current_fits
from cfa.stf.routine.run_storage import (
    FitKey,
    find_fits,
    standalone_directory,
    write_json,
)
from cfa.stf.routine.utils.collate_plots import merge_pdfs


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


def write_summaries(selected: dict[FitKey, Path], destination: Path, diseases) -> None:
    """Collate exactly the fits in the supplied snapshot."""
    for disease in diseases:
        fits = [
            path for key, path in sorted(selected.items()) if key.disease == disease
        ]
        if not fits:
            continue
        output = destination / "summary" / disease
        output.mkdir(parents=True, exist_ok=True)
        combine_hubverse_tables(fits, output, disease)
        pdfs = {}
        for fit in fits:
            for pdf in sorted((fit / "figures").glob("*.pdf")):
                pdfs.setdefault(pdf.name, []).append(pdf)
        for name, paths in pdfs.items():
            figure = output / "figures" / name
            figure.parent.mkdir(exist_ok=True)
            merge_pdfs(paths, figure)


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
        for key, path in current_fits(find_fits(base)).items()
        if key.disease in diseases
    }
    summary_dir = destination / "summary"
    summary_dir.mkdir(parents=True, exist_ok=True)
    write_json(
        summary_dir / "sources.json",
        {"/".join(key): str(path) for key, path in selected.items()},
    )
    write_summaries(selected, destination, diseases)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("base_forecast_dir", type=Path)
    parser.add_argument("--diseases", nargs="+", default=["covid", "flu", "rsv"])
    main(**vars(parser.parse_args()))
