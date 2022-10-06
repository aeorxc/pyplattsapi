import requests
import pandas as pd
import os
import time

def getMonthlyCrudeProductionByCountry(country: str, year: int):
    APP_Key = {'appkey': os.getenv('SUPPLY_API_KEY')}
    Cred_Data = {'username': os.getenv('Cred_Data_Username'), 'password': os.getenv('Cred_Data_Password')}
    token_request = requests.post("https://api.platts.com/auth/api", headers=APP_Key, data=Cred_Data)
    req_dic = token_request.json()
    access_token = req_dic['access_token']
    Bearer_Token = "Bearer " + access_token
    Headers = {'accept': 'application/json', 'appkey': os.getenv('SUPPLY_API_KEY'), 'Authorization': Bearer_Token}
    Historical_data_URL = f"https://api.platts.com/wos/v2/production?scenarioTermId=2&filter=countryname%3A%22{country}%22%20and%20productionTypeName%3A%22Production%22%20and%20Year%3A{year}%20and%20supplyTypeName%3A%22Crude%22&field=sum%28value%29&pageSize=1000&groupBy=month"
    df5 = pd.DataFrame()
    page = 1
    dataexists = 'True'
    while dataexists == 'True':
        time.sleep(1) #api can only accept 2 requests per second and 5000 per day
        data_request = requests.get(url=f'{Historical_data_URL}', headers=Headers)
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

if __name__ == '__main__':
    print(getMonthlyCrudeProductionByCountry('Saudi Arabia', 2021))
