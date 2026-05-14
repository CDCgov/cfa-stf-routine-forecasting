import datetime as dt

import pytest
from cfa.cloudops.util import check_ext_env

from cfa.stf.data import get_nssp_with_exclusion

pytestmark = pytest.mark.skipif(
    not check_ext_env(),
    reason="requires external CFA data environment",
)


@pytest.mark.parametrize(
    "exclusion_strategy,exclusion_strategy_args",
    [
        ("tail_by_target_disease", {}),
        ("tail_by_total", {}),
        ("tail_by_all_disease", {}),
        ("tail_by_n", {"n": 2}),
    ],
)
def test_get_nssp_with_exclusion_placeholder(
    exclusion_strategy: str,
    exclusion_strategy_args: dict,
) -> None:
    # For LA on 2026-05-06, the algorithm suggests excluding the last day
    # because the adjusted value is too high, but the data quality report
    # suggests excluding it because it is too low.
    get_nssp_with_exclusion(
        as_of=dt.date(2026, 5, 6),
        loc_abb="LA",
        disease="RSV",
        exclusion_strategy=exclusion_strategy,
        **exclusion_strategy_args,
    )
