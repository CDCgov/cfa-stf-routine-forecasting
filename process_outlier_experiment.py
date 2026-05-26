import datetime as dt

import plotnine as p9
import polars as pl

from cfa.stf.data.get_nnsp_with_exclusion import get_nssp_with_exclusion

local = True
result_path = "outliers_benchmark_results.parquet"
if local:
    result_path = f"local_{result_path}"


def categorize_drop_diff(df):
    return df.with_columns(
        pl.when(pl.col("drop_difference") > 0)
        .then(pl.lit("algo drops too many"))
        .when(pl.col("drop_difference") < 0)
        .then(pl.lit("algo drops too few"))
        .otherwise(pl.lit("algo drops the same"))
        .alias("drop_category")
        .cast(
            pl.Enum(
                categories=[
                    "algo drops the same",
                    "algo drops too many",
                    "algo drops too few",
                ]
            )
        )
    )


def plot_row(row, disease="Total", lookback=120):
    dat = get_nssp_with_exclusion(
        disease=disease,
        loc_abb=row["loc_abb"],
        as_of=row["reference_date"],
        start_date=row["reference_date"] - dt.timedelta(days=lookback),
        exclusion_strategy="tail_by_total",
        spread_estimator=row["spread_estimator"],
        outlier_multiplier=row["outlier_multiplier"],
        max_tail_length=row["max_tail_length"],
    )
    fig = (
        p9.ggplot(
            dat.to_pandas(), p9.aes(x="reference_date", y="value", color="exclude")
        )
        + p9.geom_line()
        + p9.scale_y_log10()
        + p9.labs(
            title=f"{row['loc_abb']} as of {row['reference_date']}",
            subtitle=f"Kelly drops: {row['kelly_drops']} | Algo drops: {row['algo_drops']}",
        )
    )
    return fig


# We should actually grab the kelly drops fresh

result = (
    pl.read_parquet(result_path)
    .rename({"n_drop_kelly": "kelly_drops", "exclude_sum": "algo_drops"})
    .filter(pl.col("algo_drops").is_not_null())
    .with_columns(
        (pl.col("algo_drops") - pl.col("kelly_drops")).alias("drop_difference")
    )
    .pipe(categorize_drop_diff)
)


drop_diff_counts = result.group_by(
    "drop_difference", "spread_estimator", "outlier_multiplier", "drop_category"
).len("n")

drop_cat_counts = result.group_by(
    "drop_category", "spread_estimator", "outlier_multiplier"
).len("n")

diff_counts_plot = (
    p9.ggplot(
        drop_diff_counts.filter(pl.col("drop_difference") != 0).to_pandas(),
        p9.aes(
            x="drop_difference",
            y="n",
            fill="drop_category",
        ),
    )
    + p9.geom_col()
    + p9.facet_grid("spread_estimator~outlier_multiplier", labeller="label_both")
    + p9.scale_y_log10()
    + p9.theme_minimal()
)

category_counts_plot = (
    p9.ggplot(
        drop_cat_counts.to_pandas(),
        p9.aes(
            x="drop_category",
            y="n",
            fill="drop_category",
        ),
    )
    + p9.geom_col()
    + p9.facet_grid("spread_estimator~outlier_multiplier", labeller="label_both")
    + p9.scale_y_log10()
    + p9.theme_minimal()
)

result.group_by("spread_estimator", "outlier_multiplier").agg(
    pl.col("drop_difference").abs().max()
).sort("drop_difference")

stubborn_group = (
    drop_cat_counts.filter(pl.col("drop_category") == "algo drops the same")
    .sort("n", "outlier_multiplier", descending=[True, False])
    .head(1)
)
ex_rows = stubborn_group.join(
    result,
    on=[
        "spread_estimator",
        "outlier_multiplier",
    ],
).filter(pl.col("drop_difference") != 0)


# [plot_row(row) for row in ex_rows.iter_rows(named=True)]

plot_row(ex_rows.row(2, named=True))
# really only concerned 2
# lesson: might have an off by one problem
# lesson: probably need to add an option for whether or not to right trunc adjust


drop_cat_counts.pivot(columns="drop_category", values="n").sort(
    "algo drops the same", descending=True
)
