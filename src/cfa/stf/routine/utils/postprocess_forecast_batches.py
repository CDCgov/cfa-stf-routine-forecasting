"""Summaries from a snapshot of completed, current fits."""

import argparse
import datetime as dt
from pathlib import Path

import polars as pl

from cfa.stf.routine.fusion import validate_fusion_sources
from cfa.stf.routine.run_storage import (
    FitKey,
    find_fits,
    standalone_directory,
)
from cfa.stf.routine.utils.collate_plots import merge_pdfs


def _hubverse_table_filename(forecast_date, disease):
    return f"{forecast_date}-{disease}-hubverse-table.parquet"


def combine_hubverse_tables(
    fits: list[Path], output: Path, disease: str, *, forecast_date: dt.date
) -> None:
    """Stream only the explicitly selected fits into a combined Parquet table."""
    if not fits:
        raise FileNotFoundError("No hubverse_table.parquet files selected")
    combined = pl.concat(
        [pl.scan_parquet(path / "hubverse_table.parquet") for path in fits],
        how="vertical_relaxed",
        parallel=False,
    )
    output.mkdir(parents=True, exist_ok=True)
    combined.sink_parquet(
        output / _hubverse_table_filename(forecast_date, disease), engine="streaming"
    )


def _fits_by_disease(selected: dict[FitKey, Path]) -> dict[str, list[Path]]:
    fits: dict[str, list[Path]] = {}
    for key, path in sorted(selected.items()):
        fits.setdefault(key.disease, []).append(path)
    return dict(sorted(fits.items()))


def write_sources_manifest(selected: dict[FitKey, Path], destination: Path) -> None:
    """Record the selected fits in the summary's sources.tsv file."""
    summary_dir = destination / "00_summary"
    summary_dir.mkdir(parents=True, exist_ok=True)
    pl.DataFrame(
        [(*key, str(path)) for key, path in sorted(selected.items())],
        schema={
            "location": pl.String,
            "disease": pl.String,
            "model": pl.String,
            "path": pl.String,
        },
        orient="row",
    ).write_csv(summary_dir / "sources.tsv", separator="\t")


def write_hubverse_summaries(
    selected: dict[FitKey, Path], destination: Path, *, forecast_date: dt.date
) -> None:
    """Combine the selected fits' Hubverse tables separately for each disease."""
    output = destination / "00_summary" / "hub_tables"
    output.mkdir(parents=True, exist_ok=True)
    for disease, fits in _fits_by_disease(selected).items():
        combine_hubverse_tables(fits, output, disease, forecast_date=forecast_date)


def merge_summary_plots(selected: dict[FitKey, Path], destination: Path) -> None:
    """Merge matching plot filenames separately for each disease."""
    output = destination / "00_summary" / "figures"
    output.mkdir(parents=True, exist_ok=True)
    for disease, fits in _fits_by_disease(selected).items():
        pdfs = {}
        for fit in fits:
            for pdf in sorted((fit / "figures").glob("*.pdf")):
                pdfs.setdefault(pdf.name, []).append(pdf)
        for name, paths in pdfs.items():
            figure = output / disease / name
            figure.parent.mkdir(parents=True, exist_ok=True)
            merge_pdfs(paths, figure)


def main(
    base_forecast_dir: Path | str,
    *,
    forecast_date: dt.date,
    execution_dir: Path | None = None,
) -> None:
    base = Path(base_forecast_dir)
    destination = execution_dir or standalone_directory(
        base, {"operation": "postprocess"}
    )
    selected = find_fits(base)
    validate_fusion_sources(selected)
    write_sources_manifest(selected, destination)
    write_hubverse_summaries(selected, destination, forecast_date=forecast_date)
    merge_summary_plots(selected, destination)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("base_forecast_dir", type=Path)
    parser.add_argument("--forecast-date", type=dt.date.fromisoformat, required=True)
    main(**vars(parser.parse_args()))
