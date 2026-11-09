#!/usr/bin/env python3

import argparse
import logging
import re
from collections import defaultdict
from pathlib import Path

from pypdf import PdfWriter

from cfa.stf.routine.fusion import current_fits
from cfa.stf.routine.run_storage import find_fits, standalone_directory


def merge_pdfs(to_merge: list[Path], output_path: Path) -> None:
    """
    Merge an ordered list of PDF files and
    write the result to a designated output path.

    Parameters
    ----------
    to_merge
        List of paths to the PDF files to merge.

    output_path
        Where to write the merged PDF.

    Returns
    -------
    None
    """
    logger = logging.getLogger(__name__)
    logger.info(f"Merging {len(to_merge)} PDFs into {output_path}")
    pdf_writer = PdfWriter()
    for pdf_file in to_merge:
        pdf_writer.append(str(pdf_file))  # pypdf expects a string path
    pdf_writer.write(str(output_path))


def collect_pdfs(model_batch_dir: Path) -> dict[str, dict[str, list[Path]]]:
    """Find and group PDFs by their shared filenames within each model folder."""
    pdf_groups = defaultdict(lambda: defaultdict(list))
    for (_, disease, model), model_path in sorted(
        current_fits(find_fits(model_batch_dir)).items()
    ):
        for file_path in sorted((model_path / "figures").glob("*.pdf")):
            signal_name = re.sub(
                f"^{re.escape(model)}_|(_epiweekly|_daily|_log|_agg_num|_agg_denom)+$",
                "",
                file_path.stem,
            )
            pdf_groups[f"{disease}/{signal_name}"][file_path.name].append(file_path)

    return pdf_groups


def merge_and_save_pdfs(model_batch_dir: Path) -> None:
    """Merge PDFs by shared name and save to model_batch_dir."""
    pdf_groups = collect_pdfs(model_batch_dir)

    if not pdf_groups:
        print("No PDFs found to merge.")
        return

    execution_dir = standalone_directory(
        model_batch_dir, {"operation": "collate_plots"}
    )
    for signal, file_dict in pdf_groups.items():
        for original_file_name, pdf_list in file_dict.items():
            output_path = execution_dir / "00_summary" / signal / original_file_name
            output_path.parent.mkdir(parents=True, exist_ok=True)
            merge_pdfs(pdf_list, output_path)


parser = argparse.ArgumentParser(
    description=("Collate forecast plots from subdirectories into single PDFs")
)

parser.add_argument(
    "model_batch_dir",
    type=Path,
    help=(
        "Base directory containing subdirectories that represent "
        "individual forecast dates, each of which in turn has "
        "subdirectories that represent individual location forecasts."
    ),
    default=None,
)

if __name__ == "__main__":
    args = parser.parse_args()
    model_batch_dir = args.model_batch_dir
    merge_and_save_pdfs(model_batch_dir)
