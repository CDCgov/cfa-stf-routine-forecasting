import datetime as dt
from pathlib import Path

import numpy as np
import polars as pl
from tqdm import tqdm

from cfa.stf.data.get_data import get_nssp
from cfa.stf.data.get_nnsp_with_exclusion import identify_outlier_tail

outliers_benchmark = pl.read_csv(
    Path.home() / "outliers_benchmark.csv", schema_overrides={"date": pl.Date}
).rename({"date": "reference_date", "state": "loc_abb", "n_drop": "n_drop_kelly"})


all_nssp = (
    pl.concat(
        [
            get_nssp(
                disease="Total", as_of=as_of, start_date=as_of - dt.timedelta(days=120)
            )
            .with_columns(pl.lit(as_of).alias("as_of"))
            .with_columns(
                offset_days=(
                    pl.col("reference_date").max() - pl.col("reference_date")
                ).dt.total_days()
                + 1
            )
            .sort("geo_value", "reference_date")
            for as_of in outliers_benchmark.unique("reference_date")
            .get_column("reference_date")
            .sort()
            .to_list()
        ]
    )
    .filter(~pl.col("geo_value").is_in(["WY", "GU"]))
    .with_columns(pl.col("geo_value").cast(pl.String))
    .collect()
)


# we actually want to right trunc adjust these, but maybe we can be lazy here

outlier_params = (
    pl.DataFrame({"spread_estimator": ["mean", "median"]})
    .join(pl.DataFrame({"outlier_multiplier": np.linspace(1, 10, 10)}), how="cross")
    .with_columns(pl.lit(7).alias("max_tail_length"))
)


def get_exclusion_count_from_row(row):
    return sum(
        identify_outlier_tail(
            x=row["value"],
            spread_estimator=row["spread_estimator"],
            outlier_multiplier=row["outlier_multiplier"],
            max_tail_length=row["max_tail_length"],
        )
    )


result = (
    all_nssp.group_by("as_of", "geo_value")
    .agg(pl.col("value"))
    .join(
        outliers_benchmark.rename({"reference_date": "as_of", "loc_abb": "geo_value"}),
        on=["as_of", "geo_value"],
        how="left",
    )
    .rename({"geo_value": "loc_abb", "as_of": "reference_date"})
    .with_columns(pl.col("n_drop_kelly").fill_null(0))
    .join(outlier_params, how="cross")
    .filter(pl.col("n_drop_kelly") <= pl.col("max_tail_length"))
    .pipe(
        lambda df: df.with_columns(
            pl.Series(
                [
                    get_exclusion_count_from_row(row)
                    for row in tqdm(df.iter_rows(named=True), total=df.height)
                ]
            ).alias("exclude_sum")
        )
    )
)

result.write_parquet("local_outliers_benchmark_results.parquet")
