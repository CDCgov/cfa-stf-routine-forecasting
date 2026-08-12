import polars as pl

from cfa.stf.routine.data.generate_test_data import (
    FIRST_OBS_DATE,
    LAST_OBS_DATE,
    LocationData,
    _make_nssp,
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
