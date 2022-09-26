import requests
import pandas as pd
import os
import time

def getCountryRunsTimeSeries(country: str):

    APP_Key = {'appkey': os.getenv('API_KEY')}
    Cred_Data = {'username': os.getenv('Cred_Data_Username'), 'password': os.getenv('Cred_Data_Password')}
    token_request = requests.post("https://api.platts.com/auth/api", headers=APP_Key, data=Cred_Data)
    req_dic = token_request.json()
    access_token = req_dic['access_token']
    Bearer_Token = "Bearer " + access_token
    Headers = {'accept': 'application/json', 'appkey': os.getenv('API_KEY'), 'Authorization': Bearer_Token}

    Historical_data_URL = f"https://api.platts.com/odata/refinery-data/v2/Runs?$select=*&$expand=*&pageSize=1000&$filter=Refinery/Country/Name eq '{country}'"

    df5 = pd.DataFrame()

    while Historical_data_URL != 'NaN':

        time.sleep(1) #api can only accept 2 requests per second and 5000 per day
        data_request = requests.get(url=f'{Historical_data_URL}', headers=Headers)
        data = data_request.json()
        df2 = pd.json_normalize(data).reset_index(drop=True)

        x = df2['value'].iloc[0]
        df3 = pd.json_normalize(x).reset_index(drop=True)
        df3 = df3.drop_duplicates()
        df4 = df3[['Year', 'Refinery.Name', 'Mbcd', 'Mmtcd', 'Mmtcy']]
        df4 = df4.groupby(['Year', 'Refinery.Name']).sum()
        df5 = df5.append(df4, ignore_index=False)

        try:
            Historical_data_URL = df2[f'@odata.nextLink'].iloc[0]

        except:

            Historical_data_URL = 'NaN'

            continue

    df5 = df5.reset_index()
    df5.columns = ['Year', 'Refinery', 'Mbdc', 'Mmtcd', 'Mmtcy']
    df7 = df5.groupby(['Year']).sum()
    df7 = df7.reset_index()

    return df7

if __name__ == '__main__':
    print(getCountryRunsTimeSeries('China'))