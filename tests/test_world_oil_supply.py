import pytest
import pandas as pd
from pyplattsapi import world_oil_supply


def test_monthly_production():
    res = world_oil_supply.production(filter="countryname=Saudi Arabia")
    assert isinstance(res, pd.DataFrame)

# def test_get_crude_supply_by_country():
#     res = pyplattsapi.world_oil_supply.getMonthlyCrudeProductionByCountry('Saudi Arabia', 2021)
#     assert len(res) == 12
#     assert 'sum_value' in res.columns