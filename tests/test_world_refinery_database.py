import pandas as pd
import pytest

from pyplattsapi import world_refinery_database


@pytest.mark.parametrize(
    "filter, field, groupBy",
    [
        (
            'RefineryId eq 1',
            None,
            None,
        ),
    ],
)
def test_get_runs(filter: str, field: str, groupBy: str):
    res = world_refinery_database.get_runs(filter=filter, field=field, groupBy=groupBy)
    assert isinstance(res, pd.DataFrame)

@pytest.mark.parametrize(
    "filter, groupBy",
    [
        (
            "Country/Name eq 'United States'",
            None,
        ),
    ],
)
def test_get_refineries(filter: str, groupBy: str):
    res = world_refinery_database.get_refineries(filter=filter, groupBy=groupBy)
    assert isinstance(res, pd.DataFrame)
