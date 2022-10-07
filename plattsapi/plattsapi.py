import requests
import pandas as pd
import time
from plattsapi import credentials as c

credentials = c.getCredentials()
Headers_sup = credentials[0]
Headers_ref = credentials[1]
Headers_inv = credentials[2]

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

def getCountryCapacityChangesTimeSeries(country: str):
    Historical_data_URL = f"https://api.platts.com/odata/refinery-data/v2/capacity?$select=*&$expand=*&pageSize=1000&$filter=Refinery/Country/Name eq '{country}'"
    df5 = pd.DataFrame()
    while Historical_data_URL != 'NaN':
        time.sleep(1)  # api can only accept 2 requests per second and 5000 per day
        data_request = requests.get(url=f'{Historical_data_URL}', headers=Headers_sup)
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
            Historical_data_URL = 'NaN'
            continue
    df4.columns = ['Date', 'Refinery', 'Mbdc', 'Mmtcd', 'Mmtcy']
    df5 = df4.groupby(['Date']).sum()
    df6 = df5.reset_index()
    return df6

def getCountryYearlyRunsTimeSeries(country: str):
    Historical_data_URL = f"https://api.platts.com/odata/refinery-data/v2/Runs?$select=*&$expand=*&pageSize=1000&$filter=Refinery/Country/Name eq '{country}'"
    df5 = pd.DataFrame()
    while Historical_data_URL != 'NaN':
        time.sleep(1) #api can only accept 2 requests per second and 5000 per day
        data_request = requests.get(url=f'{Historical_data_URL}', headers=Headers_ref)
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

def getMarginsbyType(type : str):
    Historical_data_URL = f"https://api.platts.com/odata/refinery-data/v2/Margins?&pageSize=1000&$expand=*"
    df5 = pd.DataFrame()
    while Historical_data_URL != 'NaN':
        time.sleep(1)  # api can only accept 2 requests per second and 5000 per day
        data_request = requests.get(url=f'{Historical_data_URL}', headers=Headers_ref)
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
    df5 = df5[df5['MarginType.Name'] == type]
    df5 = df5.reset_index()
    return df5

def getUSInventoriesByProduct(product : str):
    Historical_data_URL = f"https://api.platts.com/odata/inventory-data/v2/USWeeklyCrudeAndRefinedProductInventoryForecasts?$select=*&pageSize=1000"
    df5 = pd.DataFrame()
    while Historical_data_URL != 'NaN':
        time.sleep(1)  # api can only accept 2 requests per second and 5000 per day
        data_request = requests.get(url=f'{Historical_data_URL}', headers=Headers_inv)
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
    Historical_data_URL = f"https://api.platts.com/odata/inventory-data/v2/USPetroleumImportTradeStatistics?$select=*&pageSize=1000"
    df5 = pd.DataFrame()
    while Historical_data_URL != 'NaN':
        time.sleep(1)  # api can only accept 2 requests per second and 5000 per day
        data_request = requests.get(url=f'{Historical_data_URL}', headers=Headers_inv)
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
    Historical_data_URL = f"https://api.platts.com/odata/inventory-data/v2/USCrudeExports?$select=*&pageSize=1000"
    df5 = pd.DataFrame()
    while Historical_data_URL != 'NaN':
        time.sleep(1)  # api can only accept 2 requests per second and 5000 per day
        data_request = requests.get(url=f'{Historical_data_URL}', headers=Headers_inv)
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