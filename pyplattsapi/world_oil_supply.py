import time

import pandas as pd
import requests

from pyplattsapi import plattsapicore

# https://developer.platts.com/servicecatalog#/WorldOilSupply(Beta)/v2/

api_name = "WORLD OIL SUPPLY"
production_api = f"{plattsapicore.api_url}/wos/v2/production"


def get_production_api_call(
    filter: str, field: str, groupBy: str, page: int = 1, scenarioTermId: int = 2
):
    params = {
        "filter": filter,
        "scenarioTermId": scenarioTermId,
        "field": field,
        "pageSize": 1000,
        "groupBy": groupBy,
        "page": page,
    }
    response = requests.get(
        url=production_api, headers=plattsapicore.build_header(api_name), params=params
    )
    if response.status_code == 200:
        return response.json()


def get_production(filter, field, groupBy):
    r = get_production_api_call(filter, field, groupBy)
    max_page = int(r["metadata"]["count"] / 1000)

    res = pd.concat([pd.Series(x) for x in r["results"]], axis=1)
    for page in range(2, max_page + 2):
        time.sleep(0.55)
        r = get_production_api_call(filter, field, groupBy, page=page)
        d = pd.concat([pd.Series(x) for x in r["results"]], axis=1)
        res = pd.concat([d, res], axis=1)

    res = res.T
    res.index = res.apply(lambda x: pd.to_datetime(f"{x.year}-{x.month}-1"), 1)
    res.index.name = "date"
    return res
