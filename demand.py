import requests
import pandas as pd
import os
import time

def getdemandbycountry(country):

    APP_Key = {'appkey': os.getenv('API_KEY_DEMAND')}
    Cred_Data = {'username': os.getenv('Cred_Data_Username'), 'password': os.getenv('Cred_Data_Password')}
    token_request = requests.post("https://api.platts.com/auth/api", headers=APP_Key, data=Cred_Data)
    req_dic = token_request.json()
    access_token = req_dic['access_token']
    Bearer_Token = "Bearer " + access_token
    Headers = {'accept': 'application/json', 'appkey': os.getenv('API_KEY_DEMAND'), 'Authorization': Bearer_Token}
    country = 'China'
    Historical_data_URL = f"https://api.platts.com/mgodf/v1/demand?&pageSize=1000&$expand=*&filter=CountryName%20eq%20%22{country}%22"
    df5 = pd.DataFrame()
    page=1
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
        print(df3)
        page = page + 1

        try:
            Historical_data_URL = f"https://api.platts.com/mgodf/v1/demand?&pageSize=1000&$expand=*&page={page}&filter=CountryName%20eq%20%22{country}%22"

        except:

            Historical_data_URL = 'NaN'

            continue
    df5 = df5.reset_index()
    return df5

if __name__ == '__main__':
    print(getdemandbycountry('China'))

