import pandas as pd
import pytest

from pyplattsapi import world_oil_supply


@pytest.mark.parametrize(
    "filter, field, groupBy",
    [
        (
            'countryname:"Italy" and supplyTypeName:"Crude" and productionTypeName:"Production"',
            None,
            None,
        ),
        (
            'countryname:"Saudi Arabia" and supplyTypeName:"Crude" and productionTypeName:"Production"',
            "sum(value)",
            "countryName, year, month",
        )
    ],
)
def test_get_production(filter: dict, field: str, groupBy: str):
    res = world_oil_supply.get_production(filter=filter, field=field, groupBy=groupBy)
    assert isinstance(res, pd.DataFrame)
