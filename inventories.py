import requests
import pandas as pd
import os
import time

def getUSInventoriesByProduct(product : str):
    Headers = {'accept': 'application/json', 'appkey': os.getenv('API_KEY_INVENTORY')}
    Historical_data_URL = f"https://api.platts.com/odata/inventory-data/v2/USWeeklyCrudeAndRefinedProductInventoryForecasts?$select=*&pageSize=1000"
    df5 = pd.DataFrame()

    while Historical_data_URL != 'NaN':
        time.sleep(1)  # api can only accept 2 requests per second and 5000 per day
        data_request = requests.get(url=f'{Historical_data_URL}', headers=Headers)
        data = data_request.json()
        df2 = pd.json_normalize(data).reset_index(drop=True)
        x = df2['value'].iloc[0]
        df3 = pd.json_normalize(x).reset_index(drop=True)
        df3 = df3.drop_duplicates()
        df5 = df5.append(df3, ignore_index=False)
        try:
            Historical_data_URL = df2[f'@odata.nextLink'].iloc[0]
        except:
            Historical_data_URL = 'NaN'
            continue
    df5 = df5[df5['ProductName'] == product]
    df5 = df5.reset_index()
    return df5

def getUSImportsByProduct(product : str):
    Headers = {'accept': 'application/json', 'appkey': os.getenv('API_KEY_INVENTORY')}
    Historical_data_URL = f"https://api.platts.com/odata/inventory-data/v2/USPetroleumImportTradeStatistics?$select=*&pageSize=1000"
    df5 = pd.DataFrame()

    while Historical_data_URL != 'NaN':
        time.sleep(1)  # api can only accept 2 requests per second and 5000 per day
        data_request = requests.get(url=f'{Historical_data_URL}', headers=Headers)
        data = data_request.json()
        df2 = pd.json_normalize(data).reset_index(drop=True)
        x = df2['value'].iloc[0]
        df3 = pd.json_normalize(x).reset_index(drop=True)
        df3 = df3.drop_duplicates()
        df5 = df5.append(df3, ignore_index=False)
        try:
            Historical_data_URL = df2[f'@odata.nextLink'].iloc[0]
        except:
            Historical_data_URL = 'NaN'
            continue
    df5 = df5[df5['ProductName'] == product]
    df5 = df5.groupby(['ArrivalDate']).sum()
    df5 = df5.reset_index()
    return df5


def getUSCrudeExports():
    Headers = {'accept': 'application/json', 'appkey': os.getenv('API_KEY_INVENTORY')}
    Historical_data_URL = f"https://api.platts.com/odata/inventory-data/v2/USCrudeExports?$select=*&pageSize=1000"
    df5 = pd.DataFrame()

    while Historical_data_URL != 'NaN':

        time.sleep(1)  # api can only accept 2 requests per second and 5000 per day
        data_request = requests.get(url=f'{Historical_data_URL}', headers=Headers)
        data = data_request.json()
        df2 = pd.json_normalize(data).reset_index(drop=True)

        x = df2['value'].iloc[0]
        df3 = pd.json_normalize(x).reset_index(drop=True)
        df3 = df3.drop_duplicates()
        df5 = df5.append(df3, ignore_index=False)

        try:
            Historical_data_URL = df2[f'@odata.nextLink'].iloc[0]

        except:

            Historical_data_URL = 'NaN'

            continue
    df5 = df5.reset_index()
    return df5

def getChinaMDInventories():
    Headers = {'accept': 'application/json', 'appkey': os.getenv('API_KEY_INVENTORY')}
    Historical_data_URL = f"https://api.platts.com/odata/inventory-data/v2/ChinaFundamentalHeaders?&pageSize=1000&$expand=*"
    df5 = pd.DataFrame()

    while Historical_data_URL != 'NaN':

        time.sleep(1)  # api can only accept 2 requests per second and 5000 per day
        data_request = requests.get(url=f'{Historical_data_URL}', headers=Headers)
        data = data_request.json()
        df2 = pd.json_normalize(data).reset_index(drop=True)

        x = df2['value'].iloc[0]
        df3 = pd.json_normalize(x).reset_index(drop=True)
        df3 = df3.drop_duplicates()
        df5 = df5.append(df3, ignore_index=False)

        try:
            Historical_data_URL = df2[f'@odata.nextLink'].iloc[0]

        except:

            Historical_data_URL = 'NaN'

            continue
    df5 = df5.reset_index()
    return df5


if __name__ == '__main__':
    print(getChinaMDInventories())
