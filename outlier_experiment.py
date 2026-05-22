import datetime as dt
import time
from pathlib import Path

import polars as pl

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


exclusion_params = pl.DataFrame(
    {"outlier_sd_multiplier": [2.0, 2.5, 3.0]}
).with_columns(pl.lit(7).alias("max_tail_length"))
all_benchmark = (
    outliers_benchmark.unique("reference_date")
    .select("reference_date")
    .join(all_locs, how="cross")
    .join(outliers_benchmark, on=["reference_date", "loc_abb"], how="left")
    .with_columns(pl.col("n_drop_kelly").fill_null(0))
    .join(exclusion_params, how="cross")
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
                outlier_sd_multiplier=row["outlier_sd_multiplier"],
                max_tail_length=row["max_tail_length"],
            )
            .get_column("exclude")
            .sum()
        )
    except Exception as e:
        print(f"Error processing row {row}: {e}")
        return None


start_time = time.time()
result = all_benchmark.pipe(
    lambda df: df.with_columns(
        pl.Series(
            [get_exclusion_count_from_row(row) for row in df.iter_rows(named=True)]
        ).alias("exclude_sum")
    )
)
end_time = time.time()
print(f"Execution time: {end_time - start_time:.2f} seconds")

result = result.with_columns(
    predicted_positives=pl.col("exclude_sum"),
    predicted_negatives=pl.col("max_tail_length") - pl.col("exclude_sum"),
    true_positives=pl.col("n_drop_kelly"),
    true_negatives=pl.col("max_tail_length") - pl.col("n_drop_kelly"),
)

result = result.with_columns(
    true_positives=pl.min_horizontal("predicted_positives", "true_positives"),
    true_negatives=pl.min_horizontal("predicted_negatives", "true_negatives"),
)

result = result.with_columns(
    false_positives=pl.col("predicted_positives") - pl.col("true_positives"),
    false_negatives=pl.col("predicted_negatives") - pl.col("true_negatives"),
)

# Positive = Should be excluded
# Negative = Should not be excluded

# We want to be aggressive in identifying outliers, so we want high sensitivity positives and don't care about false discovery rate that much

result.group_by("max_tail_length", "outlier_sd_multiplier").agg(
    false_discovery_rate=pl.col("false_positives").sum()
    / pl.col("predicted_positives").sum(),
    sensitivity=pl.col("true_positives").sum() / pl.col("n_drop_kelly").sum(),
)


# kelly dropped vs algorithm dropped
# or bin it
# algorithdropped - kellydropped

result = (
    result.rename({"n_drop_kelly": "kelly_drops", "exclude_sum": "algo_drops"})
    .filter(pl.col("algo_drops").is_not_null())
    .with_columns(
        (pl.col("algo_drops") - pl.col("kelly_drops")).alias("drop_difference")
    )
)
result.write_parquet("outliers_benchmark_results.parquet")


result.group_by("drop_difference", "outlier_sd_multiplier").len("n").sort(
    "outlier_sd_multiplier"
).pivot(
    index="drop_difference", on="outlier_sd_multiplier", values="n", maintain_order=True
).fill_null(0).sort("drop_difference")


target_config = result.filter(pl.col("drop_difference") <= -6).row(0, named=True)

target_dat = get_nssp_with_exclusion(
    disease="Total",
    loc_abb=target_config["loc_abb"],
    as_of=target_config["reference_date"],
    start_date=target_config["reference_date"] - dt.timedelta(days=120),
    exclusion_strategy="tail_by_total",
    outlier_sd_multiplier=target_config["outlier_sd_multiplier"],
    max_tail_length=target_config["max_tail_length"],
)

import plotnine as p9

(
    p9.ggplot(
        target_dat.to_pandas(),
        p9.aes(x="reference_date", y="value", color="exclude"),
    )
    + p9.geom_line()
)
# something definitely wrong with that
