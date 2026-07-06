"""Unit tests for postprocess_forecast_batches utility functions."""

import datetime as dt
from pathlib import Path

import polars as pl
import pytest

from pipelines.utils.postprocess_forecast_batches import (
    _hubverse_table_filename,
    combine_hubverse_tables,
)


class TestHubverseTableFilename:
    """Tests for _hubverse_table_filename."""

    @pytest.mark.parametrize(
        "report_date,disease,expected",
        [
            (
                dt.date(2024, 12, 21),
                "COVID-19",
                "2024-12-21-covid-19-hubverse-table.parquet",
            ),
            (
                "2024-12-21",
                "Influenza",
                "2024-12-21-influenza-hubverse-table.parquet",
            ),
            (
                dt.date(2025, 1, 5),
                "RSV",
                "2025-01-05-rsv-hubverse-table.parquet",
            ),
        ],
    )
    def test_hubverse_table_filename(self, report_date, disease, expected):
        assert _hubverse_table_filename(report_date, disease) == expected


class TestCombineHubverseTables:
    """Tests for combine_hubverse_tables."""

    def _make_batch_dir(self, tmp_path: Path) -> Path:
        """Create a model batch directory with a valid name."""
        batch_dir = tmp_path / "covid-19_r_2024-12-21_f_2024-09-22_t_2024-12-20"
        batch_dir.mkdir()
        return batch_dir

    def _write_hubverse_table(self, parent: Path, df: pl.DataFrame) -> Path:
        """Write a hubverse_table.parquet inside parent directory."""
        parent.mkdir(parents=True, exist_ok=True)
        path = parent / "hubverse_table.parquet"
        df.write_parquet(path)
        return path

    def test_combine_raises_when_no_tables_found(self, tmp_path):
        """combine_hubverse_tables raises FileNotFoundError when no tables found."""
        batch_dir = self._make_batch_dir(tmp_path)

        with pytest.raises(FileNotFoundError, match="No hubverse_table.parquet files"):
            combine_hubverse_tables(batch_dir)

    def test_combine_creates_output_file(self, tmp_path):
        """combine_hubverse_tables creates the expected output parquet file."""
        batch_dir = self._make_batch_dir(tmp_path)

        df = pl.DataFrame({"location": ["CA"], "value": [1.0]})
        self._write_hubverse_table(batch_dir / "loc_CA", df)

        combine_hubverse_tables(batch_dir)

        output = batch_dir / "2024-12-21-covid-19-hubverse-table.parquet"
        assert output.exists()

    def test_combine_concatenates_multiple_tables(self, tmp_path):
        """combine_hubverse_tables concatenates tables from multiple subdirs."""
        batch_dir = self._make_batch_dir(tmp_path)

        df_ca = pl.DataFrame({"location": ["CA"], "value": [1.0]})
        df_tx = pl.DataFrame({"location": ["TX"], "value": [2.0]})
        self._write_hubverse_table(batch_dir / "loc_CA", df_ca)
        self._write_hubverse_table(batch_dir / "loc_TX", df_tx)

        combine_hubverse_tables(batch_dir)

        output = batch_dir / "2024-12-21-covid-19-hubverse-table.parquet"
        result = pl.read_parquet(output)
        assert result.shape[0] == 2
        assert set(result["location"].to_list()) == {"CA", "TX"}

    def test_combine_recursive_search(self, tmp_path):
        """combine_hubverse_tables finds hubverse_table.parquet files recursively."""
        batch_dir = self._make_batch_dir(tmp_path)

        df = pl.DataFrame({"location": ["WA"], "value": [3.0]})
        self._write_hubverse_table(batch_dir / "deep" / "nested" / "loc_WA", df)

        combine_hubverse_tables(batch_dir)

        output = batch_dir / "2024-12-21-covid-19-hubverse-table.parquet"
        result = pl.read_parquet(output)
        assert result.shape[0] == 1
        assert result["location"][0] == "WA"
