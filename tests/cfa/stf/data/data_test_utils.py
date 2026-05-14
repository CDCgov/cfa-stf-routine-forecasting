def _unique_values(df, column: str) -> set[str]:
    if hasattr(df, "collect"):
        df = df.collect()
    return set(df.unique(column).get_column(column).to_list())
