import pandas as pd

from pyplattsapi import plattsapicore

# https://developer.platts.com/servicecatalog#/Mgodf(Beta)/v1/

api_name = "WORLD OIL DEMAND"
production_api = f"{plattsapicore.api_url}/mgodf/v1/demand"

def get_demand(filter, field, groupBy):
    params = {
        "filter": filter,
        "field": field,
        "pageSize": 1000,
        "groupBy": groupBy,
        "page": 1,
    }
    res = plattsapicore.generic_api_call(api=production_api, api_name=api_name, params=params)

    if 'month' in res.columns and 'year' in res.columns:
        res.index = res.apply(lambda x: pd.to_datetime(f"{x.year}-{x.month}-1"), 1)
        res.index.name = "date"
    return res
