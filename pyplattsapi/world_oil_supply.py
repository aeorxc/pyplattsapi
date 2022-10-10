import time
import pandas as pd
import requests
from pyplattsapi import plattsapicore

# https://developer.platts.com/servicecatalog#/WorldOilSupply(Beta)/v2/

api_name = "WORLD OIL SUPPLY"
production_api = f"{plattsapicore.api_url}/wos/v2/production"

def make_filter(filter: dict):
    filterString = 'countryname:"' + filter['countryName'] + '" and productionTypeName:"' + filter['productionType'] + '" and Year:' + filter['year'] + ' and supplyTypeName:"' + filter['product'] + '"'
    return filterString

def productionAPICall(filter: dict, field:str, groupBy:str, page: int=1 , scenarioTermId:int=2):
    params = {
        'filter' : make_filter(filter),
        'scenarioTermId': scenarioTermId,
        'field': field,
        'pageSize': 1000,
        'groupBy': groupBy,
        'page' : page,
        'select' : '*',
    }
    data_request = requests.get(url=production_api, headers=plattsapicore.build_header(api_name), params=params)
    data = data_request.json()
    data = pd.json_normalize(data).reset_index(drop=True)
    return data

def getproduction(filter, field, groupBy):
    res = pd.DataFrame()
    page = 1
    dataexists = 'True'
    while dataexists == 'True':
        time.sleep(1) #api can only accept 2 requests per second and 5000 per day
        df = productionAPICall(filter , field, groupBy, page = page)
        x = df['results'].iloc[0]
        if len(x) == 0:
            dataexists = 'False'
        df = pd.json_normalize(x).reset_index(drop=True)
        df = df.drop_duplicates()
        res = pd.concat([res,df], ignore_index=False)
        page = page + 1
    res = res.reset_index()
    res['year'] = filter['year']
    res['day'] = 1
    res['date'] = pd.to_datetime(res[['year','month','day']])
    res['date'] = res['date'].apply(lambda x: x.strftime('%Y-%m'))
    res.set_index('date', inplace= True)
    res.drop(['year','day','month','index'],inplace = True, axis=1)
    return res