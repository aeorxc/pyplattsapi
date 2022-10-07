import pandas as pd
import pytest

from pyplattsapi import world_oil_supply


@pytest.mark.parametrize(
    "filter, field, groupBy",
    [
        ("countryname=Saudi Arabia", "sum(value)", None)
    ],
)
def test_monthly_production(filter: str, field: str, groupBy: str):
    res = world_oil_supply.production(filter=filter, field=field, groupBy=groupBy)
    assert isinstance(res, pd.DataFrame)

# def test_get_crude_supply_by_country():
#     res = pyplattsapi.world_oil_supply.getMonthlyCrudeProductionByCountry('Saudi Arabia', 2021)
#     assert len(res) == 12
#     assert 'sum_value' in res.columns
