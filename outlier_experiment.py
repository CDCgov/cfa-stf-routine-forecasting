import datetime as dt
from pathlib import Path

import polars as pl
from tqdm import tqdm

from cfa.stf.data.get_data import get_nssp
from cfa.stf.data.get_nnsp_with_exclusion import get_nssp_with_exclusion

outliers_benchmark = pl.read_csv(
    Path.home() / "outliers_benchmark.csv", schema_overrides={"date": pl.Date}
).rename({"date": "reference_date", "state": "loc_abb", "n_drop": "n_drop_kelly"})
all_locs = (
    get_nssp()
    .unique("geo_value")
    .with_columns(pl.col("geo_value").cast(pl.String).alias("loc_abb"))
    .select("loc_abb")
    .sort("loc_abb")
    .filter(~pl.col("loc_abb").is_in(["WY", "GU"]))
    .collect()
)


exclusion_params = pl.DataFrame({"outlier_multiplier": [7, 8, 9]}).with_columns(
    pl.lit(7).alias("max_tail_length"), pl.lit("mean").alias("spread_estimator")
)

all_benchmark = (
    outliers_benchmark.unique("reference_date")
    .select("reference_date")
    .join(all_locs, how="cross")
    .join(outliers_benchmark, on=["reference_date", "loc_abb"], how="left")
    .with_columns(pl.col("n_drop_kelly").fill_null(0))
    .join(exclusion_params, how="cross")
    .filter(pl.col("n_drop_kelly") <= pl.col("max_tail_length"))
)


def get_exclusion_count_from_row(row):
    try:
        return (
            get_nssp_with_exclusion(
                disease="COVID-19",
                loc_abb=row["loc_abb"],
                as_of=row["reference_date"],
                start_date=row["reference_date"] - dt.timedelta(days=120),
                exclusion_strategy="tail_by_total",
                spread_estimator=row["spread_estimator"],
                outlier_multiplier=row["outlier_multiplier"],
                max_tail_length=row["max_tail_length"],
            )
            .get_column("exclude")
            .sum()
        )
    except Exception as e:
        print(f"Error processing row {row}: {e}")
        return None


result = all_benchmark.pipe(
    lambda df: df.with_columns(
        pl.Series(
            [
                get_exclusion_count_from_row(row)
                for row in tqdm(df.iter_rows(named=True), total=df.height)
            ]
        ).alias("exclude_sum")
    )
)

result.write_parquet("outliers_benchmark_results.parquet")
