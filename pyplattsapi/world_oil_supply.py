import pandas as pd

from pyplattsapi import plattsapicore

# https://developer.platts.com/servicecatalog#/WorldOilSupply(Beta)/v2/

api_name = "WORLD OIL SUPPLY"
production_api = f"{plattsapicore.api_url}/wos/v2/production"


def get_production(filter, field, groupBy, scenarioTermId: int = 2):
    params = {
        "filter": filter,
        "scenarioTermId": scenarioTermId,
        "field": field,
        "pageSize": 1000,
        "groupBy": groupBy,
        "page": 1,
    }
    res = plattsapicore.generic_api_call(api=production_api, api_name=api_name, params=params)

    res.index = res.apply(lambda x: pd.to_datetime(f"{x.year}-{x.month}-1"), 1)
    res.index.name = "date"
    return res
