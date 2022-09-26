import requests
import pandas as pd
import os
import time

#currently only providing capacity when there was a change
def getCountryCapacityTimeSeries(country: str):

    APP_Key = {'appkey': os.getenv('API_KEY')}
    Cred_Data = {'username': os.getenv('Cred_Data_Username'), 'password': os.getenv('Cred_Data_Password')}
    token_request = requests.post("https://api.platts.com/auth/api", headers=APP_Key, data=Cred_Data)
    req_dic = token_request.json()
    access_token = req_dic['access_token']
    Bearer_Token = "Bearer " + access_token
    Headers = {'accept': 'application/json', 'appkey': os.getenv('API_KEY'), 'Authorization': Bearer_Token}

    Historical_data_URL = f"https://api.platts.com/odata/refinery-data/v2/capacity?$select=*&$expand=*&pageSize=1000&$filter=Refinery/Country/Name eq '{country}'"

    df5 = pd.DataFrame()

    while Historical_data_URL != 'NaN':

        time.sleep(1)  # api can only accept 2 requests per second and 5000 per day
        data_request = requests.get(url=f'{Historical_data_URL}', headers=Headers)
        data = data_request.json()
        df2 = pd.json_normalize(data).reset_index(drop=True)

        x = df2['value'].iloc[0]
        df3 = pd.json_normalize(x).reset_index(drop=True)
        df3 = df3.drop_duplicates()
        df3['Date'] = df3[['Year', 'Quarter']].apply(lambda row: '-Q'.join(row.values.astype(str)), axis=1)
        df4 = df3[['Refinery.Name', 'Mbcd', 'Mmtcd', 'Mmtcy', 'Date']]
        df4 = df4.groupby(['Date', 'Refinery.Name']).sum()
        df4 = df4.reset_index()
        df4['Date'] = pd.to_datetime(df4['Date'])
        df5 = df5.append(df4, ignore_index=False)

        try:

            Historical_data_URL = df2[f'@odata.nextLink'].iloc[0]

        except:

            Historical_data_URL = 'NaN' \
                                  ''
            continue

    df4.columns = ['Date', 'Refinery', 'Mbdc', 'Mmtcd', 'Mmtcy']
    df5 = df4.groupby(['Date']).sum()
    df6 = df5.reset_index()

    return df6

if __name__ == '__main__':
    print(getCountryCapacityTimeSeries('China'))