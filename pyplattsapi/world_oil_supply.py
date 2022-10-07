import time

import pandas as pd
import requests
from pyplattsapi import plattsapicore

# https://developer.platts.com/servicecatalog#/WorldOilSupply(Beta)/v2/

api_name = "WORLD OIL SUPPLY"
production_api = f"{plattsapicore.api_url}/wos/v2/production"


def production(filter:str, field:str='sum(value)', groupBy:str='month', scenarioTermId:int=2):
    params = {
        'filter' : filter,
        'scenarioTermId': scenarioTermId,
        'field': field,
        'pageSize': 1000,
        'groupBy': groupBy,
    }
    data_request = requests.get(url=production_api, headers=plattsapicore.build_header(api_name),params=params)
    data = data_request.json()
    return data


def getMonthlyCrudeProductionByCountry(country: str, year: int):
    Historical_data_URL = f"https://api.platts.com/wos/v2/production?scenarioTermId=2&filter=countryname%3A%22{country}%22%20and%20productionTypeName%3A%22Production%22%20and%20Year%3A{year}%20and%20supplyTypeName%3A%22Crude%22&field=sum%28value%29&pageSize=1000&groupBy=month"
    df5 = pd.DataFrame()
    page = 1
    dataexists = 'True'
    while dataexists == 'True':
        time.sleep(1) #api can only accept 2 requests per second and 5000 per day
        data_request = requests.get(url=f'{Historical_data_URL}', headers=Headers_sup)
        data = data_request.json()
        df2 = pd.json_normalize(data).reset_index(drop=True)
        x = df2['results'].iloc[0]
        if len(x) == 0:
            dataexists = 'False'
        df3 = pd.json_normalize(x).reset_index(drop=True)
        df3 = df3.drop_duplicates()
        df5 = df5.append(df3, ignore_index=False)
        page = page + 1
        try:
            Historical_data_URL = f"https://api.platts.com/wos/v2/production?scenarioTermId=2&filter=countryname%3A%22{country}%22%20and%20productionTypeName%3A%22Production%22%20and%20Year%3A{year}%20and%20supplyTypeName%3A%22Crude%22&field=sum%28value%29&pageSize=1000&groupBy=month&page={page}"
        except:
            Historical_data_URL = 'NaN'
            continue
    df5 = df5.reset_index()
    df5['year'] = year
    df5['day'] = 1
    df5['date'] = pd.to_datetime(df5[['year','month','day']])
    df5.set_index('date', inplace= True)
    df5.drop(['year','day','month','index'],inplace = True, axis=1)
    return df5
