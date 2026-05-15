import pytest
from cfa.cloudops.util import check_ext_env

skip_without_ext_env = pytest.mark.skipif(
    not check_ext_env(),
    reason="requires external CFA data environment",
)

catalog_test = pytest.mark.catalog


def _unique_values(df, column: str) -> set[str]:
    if hasattr(df, "collect"):
        df = df.collect()
    return set(df.unique(column).get_column(column).to_list())
