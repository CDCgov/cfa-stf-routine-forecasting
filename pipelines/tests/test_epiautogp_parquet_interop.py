"""Cross-language checks for EpiAutoGP samples parquet output."""

import datetime as dt
import json
import textwrap
from pathlib import Path

import polars as pl
import pytest

from pipelines.utils.common_utils import run_julia_code, run_r_code

EXPECTED_DATES = [
    dt.date(2024, 2, 4),
    dt.date(2024, 2, 10),
    dt.date(2024, 2, 4),
    dt.date(2024, 2, 10),
]


def _quote_for_embedded_code(value: str | Path) -> str:
    return json.dumps(str(value))


def _skip_if_r_packages_missing(*packages: str) -> None:
    package_list = ", ".join(_quote_for_embedded_code(package) for package in packages)
    code = textwrap.dedent(
        f"""
        pkgs <- c({package_list})
        missing <- pkgs[!vapply(pkgs, requireNamespace, logical(1), quietly = TRUE)]
        if (length(missing) > 0) {{
          cat(paste(missing, collapse = ","))
          quit(status = 1)
        }}
        """
    )
    try:
        run_r_code(code, executor_flags=["--vanilla"], text=True)
    except (FileNotFoundError, RuntimeError) as exc:
        pytest.skip(f"R packages are not available: {exc}")


@pytest.fixture(scope="module")
def epiautogp_samples_path(tmp_path_factory) -> Path:
    model_fit_dir = (
        tmp_path_factory.mktemp("epiautogp-parquet-interop")
        / "covid-19_r_2024-02-03_f_2024-01-01_t_2024-02-01"
        / "model_runs"
        / "US"
        / "epiautogp_nhsn_epiweekly"
    )
    forecast_dates = list(dict.fromkeys(EXPECTED_DATES))
    assert len(EXPECTED_DATES) % len(forecast_dates) == 0
    draw_count = len(EXPECTED_DATES) // len(forecast_dates)
    assert EXPECTED_DATES == forecast_dates * draw_count

    forecast_dates_julia = ", ".join(
        f'Date("{forecast_date.isoformat()}")' for forecast_date in forecast_dates
    )
    forecasts_julia = "; ".join(
        " ".join(
            str(float(100 * (date_index + 1) + draw_index + 1))
            for draw_index in range(draw_count)
        )
        for date_index in range(len(forecast_dates))
    )
    julia_code = textwrap.dedent(
        f"""
        using Dates
        using EpiAutoGP

        input = EpiAutoGPInput(
            [Date("2024-01-01")],
            [100.0],
            "COVID-19",
            "US",
            "nhsn",
            "epiweekly",
            "observed",
            Date("2024-02-03"),
            Date[],
            Vector{{Real}}[]
        )
        results = (
            forecast_dates = [{forecast_dates_julia}],
            forecasts = [{forecasts_julia}],
        )

        create_forecast_output(
            input,
            results,
            {_quote_for_embedded_code(model_fit_dir)},
            PipelineOutput();
            save_output = true
        )
        """
    )
    try:
        run_julia_code(
            julia_code,
            executor_flags=["--project=EpiAutoGP", "--startup-file=no"],
            text=True,
        )
    except FileNotFoundError:
        pytest.skip("julia is not available")

    samples_path = model_fit_dir / "samples.parquet"
    assert samples_path.is_file()
    return samples_path


def test_epiautogp_samples_parquet_date_is_discovered_by_polars(
    epiautogp_samples_path,
):
    samples = pl.read_parquet(epiautogp_samples_path)

    assert samples.schema["date"] == pl.Date
    assert samples["date"].to_list() == EXPECTED_DATES

    lazy_schema = (
        pl.scan_parquet(epiautogp_samples_path)
        .select(pl.col("date").min())
        .collect()
        .schema
    )
    assert lazy_schema["date"] == pl.Date


def test_epiautogp_samples_parquet_date_is_discovered_by_r_dplyr(
    epiautogp_samples_path,
):
    _skip_if_r_packages_missing("forecasttools", "dplyr", "lubridate")
    expected_dates_r = ", ".join(
        _quote_for_embedded_code(expected_date.isoformat())
        for expected_date in EXPECTED_DATES
    )
    r_code = textwrap.dedent(
        f"""
        samples <- forecasttools::read_tabular({_quote_for_embedded_code(epiautogp_samples_path)})
        stopifnot(inherits(samples$date, "Date"))
        stopifnot(identical(
          as.character(samples$date),
          c({expected_dates_r})
        ))

        mutated <- dplyr::mutate(
          samples,
          date_plus_one = .data$date + lubridate::days(1)
        )
        stopifnot(inherits(mutated$date_plus_one, "Date"))

        summarised <- dplyr::summarise(mutated, min_date = min(.data$date))
        stopifnot(inherits(summarised$min_date, "Date"))
        """
    )

    try:
        run_r_code(r_code, executor_flags=["--vanilla"], text=True)
    except FileNotFoundError:
        pytest.skip("Rscript is not available")
