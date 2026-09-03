import datetime as dt

import numpy as np
import polars as pl
import pytest
from tests.integration.generate_test_data import (
    FIRST_OBS_DATE,
    HUBVERSE_NOWCAST_DIR_NAME,
    LAST_OBS_DATE,
    REPORT_DATE,
    LocationData,
    _make_hubverse_nowcast_rows,
    _make_nssp,
    write_hubverse_nowcast,
)

from cfa.stf.routine.data.hubverse_nowcast import (
    HUBVERSE_MODEL_OUTPUT_SUBDIR,
)


def test_make_nssp_returns_location_level_cfa_stf_data_schema():
    locations = [
        LocationData(abbr="CA", population=100_000),
        LocationData(abbr="US", population=1_000_000),
    ]
    diseases = ["covid", "flu"]

    result = _make_nssp(locations=locations, diseases=diseases)

    n_observation_dates = (LAST_OBS_DATE - FIRST_OBS_DATE).days + 1
    assert result.columns == [
        "date",
        "state_abb",
        "disease",
        "target_type",
        "value",
    ]
    assert result.height == (len(locations) * n_observation_dates * (len(diseases) + 1))
    assert result.select("date", "state_abb", "disease").n_unique() == result.height
    assert result.get_column("target_type").unique().to_list() == ["inc ed visits"]

    totals = result.filter(pl.col("disease") == "total").select(
        "date",
        "state_abb",
        total=pl.col("value"),
    )
    disease_sums = (
        result.filter(pl.col("disease") != "total")
        .group_by("date", "state_abb")
        .agg(disease_sum=pl.col("value").sum())
    )
    assert (
        totals.join(disease_sums, on=["date", "state_abb"])
        .select(pl.col("total") > pl.col("disease_sum"))
        .to_series()
        .all()
    )


@pytest.mark.parametrize(("n_selected", "n_nowcast"), [(5, 3), (6, 4)])
def test_hubverse_nowcasts_use_selected_nhsn_observations(n_selected, n_nowcast):
    first_date = dt.date(2026, 7, 4)
    selected_dates = [
        first_date + dt.timedelta(weeks=index) for index in range(n_selected)
    ]
    selected_observations = pl.DataFrame(
        {"date": selected_dates, "value": [100.0] * n_selected}
    )

    nowcasts = _make_hubverse_nowcast_rows(
        location="CA",
        disease="covid",
        nhsn_observations=selected_observations,
        rng=np.random.default_rng(12345),
    )

    assert (
        nowcasts.get_column("target_end_date").unique().sort().to_list()
        == (selected_dates[-n_nowcast:])
    )
    mean_nowcasts = (
        nowcasts.group_by("target_end_date")
        .agg(pl.col("value").mean())
        .sort("target_end_date")
        .get_column("value")
        .to_numpy()
    )
    expected_means = np.linspace(101.0, 110.0, n_nowcast)
    np.testing.assert_allclose(mean_nowcasts, expected_means, rtol=0.01)


def test_write_hubverse_nowcast_writes_data_and_figure(tmp_path):
    observations = pl.DataFrame(
        {
            "date": [
                dt.date(2026, 7, 4) + dt.timedelta(weeks=index) for index in range(6)
            ],
            "value": [100.0] * 6,
        }
    )

    write_hubverse_nowcast(
        tmp_path,
        disease="covid",
        location="CA",
        nhsn_observations=observations,
    )

    output_dir = (
        tmp_path
        / "private_data"
        / HUBVERSE_NOWCAST_DIR_NAME
        / "covid"
        / HUBVERSE_MODEL_OUTPUT_SUBDIR
    )
    artifact_stem = f"{REPORT_DATE}-CFA-nowcastNHSN"
    assert (output_dir / f"{artifact_stem}.parquet").is_file()
    assert (output_dir / f"{artifact_stem}.png").is_file()
