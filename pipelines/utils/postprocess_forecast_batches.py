#!/usr/bin/env python3

"""
Postprocess batches of forecasts, creating
summary files including collated PDFs of plots
and .tsv-format hubverse tables.
"""

import argparse
import datetime as dt
import logging
import shutil
from pathlib import Path

import polars as pl

from pipelines.utils import collate_plots as cp
from pipelines.utils.common_utils import (
    get_all_forecast_dirs,
    parse_model_batch_dir_name,
)


def _hubverse_table_filename(report_date: str | dt.date, disease: str) -> str:
    return f"{report_date}-{disease.lower()}-hubverse-table.parquet"


def combine_hubverse_tables(model_batch_dir_path: str | Path) -> None:
    model_batch_dir_path = Path(model_batch_dir_path)
    model_batch_dir_name = model_batch_dir_path.name
    batch_info = parse_model_batch_dir_name(model_batch_dir_name)

    output_file_name = _hubverse_table_filename(
        batch_info["report_date"], batch_info["disease"]
    )

    output_path = Path(model_batch_dir_path, output_file_name)
    expected_file_name = "hubverse_table.parquet"
    parquet_files = list(model_batch_dir_path.rglob(expected_file_name))
    parquet_files.sort()
    if not parquet_files:
        raise FileNotFoundError(
            f"No {expected_file_name} files found under {model_batch_dir_path}"
        )
    pl.scan_parquet(
        parquet_files, cast_options=pl.ScanCastOptions(integer_cast="allow-float")
    ).sink_parquet(output_path)
    return None


def process_model_batch_dir(model_batch_dir_path: Path) -> None:
    logger = logging.getLogger(__name__)
    logger.info("Collating plots...")
    cp.merge_and_save_pdfs(model_batch_dir_path)
    logger.info("Creating hubverse table...")
    combine_hubverse_tables(model_batch_dir_path)


def model_batch_dir_to_target_path(
    model_batch_dir: str,
    max_last_training_date: dt.date,
    pre_path: Path | str,
) -> Path:
    parts = parse_model_batch_dir_name(model_batch_dir)
    lookback = (parts["last_training_date"] - parts["first_training_date"]).days + 1
    omit = (max_last_training_date - parts["last_training_date"]).days + 1
    target_path = Path(
        pre_path,
        f"lookback-{lookback}-omit-{omit}",
        parts["disease"],
    )
    return target_path


def main(
    base_forecast_dir: Path | str,
    diseases: list[str] | set[str] = ["COVID-19", "Influenza", "RSV"],
    skip_existing: bool = True,
    local_copy_dir: Path | str = "",
) -> None:
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)
    to_process = get_all_forecast_dirs(base_forecast_dir, diseases)
    # compute max last training date across all model batch dirs and assume this corresponds to omitting 1 day.
    max_last_training_date = max(
        [
            parse_model_batch_dir_name(model_batch_dir)["last_training_date"]
            for model_batch_dir in to_process
        ]
    )
    if skip_existing:
        to_process = [
            batch_dir
            for batch_dir in to_process
            if not any(
                Path(base_forecast_dir, batch_dir).glob("*-hubverse-table.parquet")
            )
        ]

    for batch_dir in to_process:
        model_batch_dir_path = Path(base_forecast_dir, batch_dir)
        logger.info(f"Processing {batch_dir}...")
        process_model_batch_dir(model_batch_dir_path)
        logger.info(f"Finished processing {batch_dir}")
        if local_copy_dir:
            source_dir = Path(base_forecast_dir, batch_dir, "figures")
            target_dir = model_batch_dir_to_target_path(
                batch_dir, max_last_training_date, local_copy_dir
            )
            logger.info(
                f"Copying from {source_dir.relative_to(base_forecast_dir)} to {target_dir.relative_to(local_copy_dir)}..."
            )
            shutil.copytree(source_dir, target_dir, dirs_exist_ok=True)
    logger.info(f"Finished processing {base_forecast_dir}.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Postprocess forecasts across locations."
    )
    parser.add_argument(
        "base_forecast_dir",
        type=Path,
        help="Directory containing forecast subdirectories.",
    )
    parser.add_argument(
        "--diseases",
        type=str,
        default="COVID-19 Influenza RSV",
        help=(
            "Name(s) of disease(s) to postprocess, "
            "as a whitespace-separated string. Supported "
            "values are 'COVID-19' , 'RSV' and 'Influenza'. "
            "Default 'COVID-19 Influenza RSV' (i.e. postprocess all)."
        ),
    )
    parser.add_argument(
        "--skip-existing",
        action="store_true",
        help="Skip processing for model batch directories that already have been processed.",
    )
    parser.add_argument(
        "--local-copy-dir",
        type=str,
        default="",
        help="Save a local copy of the processed files to this directory, if supplied.",
    )

    args = parser.parse_args()
    args.diseases = args.diseases.split()
    main(**vars(args))
