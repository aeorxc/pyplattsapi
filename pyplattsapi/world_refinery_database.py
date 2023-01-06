import pandas as pd
from pyplattsapi import plattsapicore

api_name = "WORLD REFINERY DATABASE"
runs_api = f"{plattsapicore.api_url}/odata/refinery-data/v2/Runs"
outages_api = f"{plattsapicore.api_url}/refinery-data/v1/outage-alerts"
capacity_api = f"{plattsapicore.api_url}/odata/refinery-data/v2/Capacity"
refinery_api = f"{plattsapicore.api_url}/odata/refinery-data/v2/Refineries"


def get_runs(filter: str, field: str = None, groupBy: str = None):
    params = {
        "$filter": filter,
        "pageSize": 1000,
        "groupBy": groupBy,

    }
    res = plattsapicore.generic_odata_call(api=runs_api, api_name=api_name, params=params)

    qmap = {1: 1, 2: 4, 3: 7, 4: 10}
    res.index = res.apply(lambda x: pd.to_datetime(f"{x.Year}-{qmap.get(x.Quarter)}-1"), 1)
    res.index.name = "date"
    return res

def get_refineries(filter: str, groupBy: str = None):
    params = {
        "$filter": filter,
        "pageSize": 1000,
        "groupBy": groupBy,

    }
    res = plattsapicore.generic_odata_call(api=refinery_api, api_name=api_name, params=params)
    return res


def get_outages(filter):
    params = {
        "pageSize": 1000,
        "filter": filter,
        "page": 1,
    }
    res = plattsapicore.no_token_api_call(api=outages_api, api_name=api_name, params=params)
    df = res['alerts'].apply(lambda col: col[0]).apply(pd.Series)
    res.drop(['alerts'], inplace=True, axis=1)
    res = pd.concat([res, df], axis=1)
    res['startDate'] = pd.to_datetime(res['startDate'], format='%Y-%m-%d')
    res['endDate'] = pd.to_datetime(res['endDate'], format='%Y-%m-%d')
    return res

def get_capacity(filter):
    apply = f"filter({filter})/aggregate(Mbcd with sum as SumMbcd)"

    params = {
        "pageSize": 1000,
        "page": 1,
        "$apply": apply
    }
    res = plattsapicore.generic_odata_call(api=capacity_api, api_name=api_name, params=params)
    res = res.loc[0]['SumMbcd']
    return res
