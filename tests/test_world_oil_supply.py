import pandas as pd
import pytest

from pyplattsapi import world_oil_supply


@pytest.mark.parametrize(
    "filter, field, groupBy",
    [
        ({'countryName': 'Saudi Arabia',
          'productionType': 'Production',
          'year': '2021',
          'product': 'crude'
          }, "sum(value)", "month")
    ],
)
def test_monthly_production(filter: dict, field: str, groupBy: str):
    res = world_oil_supply.production(filter=filter, field=field, groupBy=groupBy)
    assert isinstance(res, pd.DataFrame)

@pytest.mark.parametrize(
    "filter, field, groupBy",
    [
        ({'countryName': 'Saudi Arabia',
          'productionType': 'Production',
          'year': '2021',
          'product': 'crude'
          }, "sum(value)", "month")
    ],
)
def test_1(filter: dict, field: str, groupBy: str):
    res = world_oil_supply.getMonthlyCrudeProductionByCountry(filter=filter, field=field, groupBy=groupBy)
    assert isinstance(res, pd.DataFrame)
    assert len(res) == 12
    assert 'sum_value' in res.columns
