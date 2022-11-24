import pandas as pd
from pyplattsapi import plattsapicore
from commodplot import commodplot as cpl
from commodplot import jinjautils as ju
from datetime import datetime
from qe import qe

api_name = "WORLD REFINERY DATABASE"
runs_api = f"{plattsapicore.api_url}/odata/refinery-data/v2/Runs"
outages_api = f"{plattsapicore.api_url}/refinery-data/v1/outage-alerts"
capacity_api = f"{plattsapicore.api_url}/odata/refinery-data/v2/Capacity"


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


def create_timeseries(res):
    df1 = pd.DataFrame()
    for index, row in res.iterrows():
        row = tar_to_timeseries(row['outageVol_MBD'], row['startDate'], row['endDate'])
        df1 = pd.concat([df1, row], axis=1)
    df1 = (df1.sum(axis=1))
    df1 = df1.rename('capacityOffline')
    df1 = df1.to_frame()
    return df1


def tar_to_timeseries(taramount, startdate, enddate, tarname=None):
    dr = pd.date_range("01/01/2000", "12/01/2030")
    ser = pd.Series(0, index=dr)
    ser[startdate:enddate] = taramount
    ser.name = tarname
    return ser


def display(df, country, unitType):
    fig = cpl.seas_line_plot(df, visible_line_years=1, yaxis_title="Mbbls",
                             inc_change_sum=False, title=f"{country} {unitType}")
    return fig


def create_graph(filter, country, unitType):
    df = get_outages(filter)
    df = create_timeseries(df)
    df = df[~(df.index < datetime.strptime("01/01/2017 00:00:00", '%m/%d/%Y %H:%M:%S'))]
    year = datetime.today().year + 1
    df = df[~(df.index > datetime.strptime(f"12/31/{year} 00:00:00", '%m/%d/%Y %H:%M:%S'))]
    fig = display(df, country, unitType)
    return fig


def get_jinja_dict():
    data = {"name": "Platts Offline Capacity", "title": "Platts Offline Capacity",
            "us cdu": create_graph('countryName:"United States" AND processUnitName:"CDU"',"USA", "CDU"),
            "us fcu": create_graph('countryName:"United States" AND processUnitName:"FCU"',"USA", "FCU"),
            "us coker": create_graph('countryName:"United States" AND processUnitName:"Coker"',"USA", "Coker"),
            "china cdu": create_graph('countryName:"China" AND processUnitName:"CDU"',"China", "CDU"),
            "china fcu": create_graph('countryName:"China" AND processUnitName:"FCU"',"China", "FCU"),
            "china coker": create_graph('countryName:"China" AND processUnitName:"Coker"',"China","Coker"),
            "russia cdu": create_graph('countryName:"Russia" AND processUnitName:"CDU"',"Russia","CDU"),
            "russia fcu": create_graph('countryName:"Russia" AND processUnitName:"FCU"',"Russia","FCU"),
            "russia coker": create_graph('countryName:"Russia" AND processUnitName:"Coker"',"Russia","Coker"),
            "india cdu": create_graph('countryName:"India" AND processUnitName:"CDU"',"India","CDU"),
            "india fcu": create_graph('countryName:"India" AND processUnitName:"FCU"',"India","FCU"),
            "india coker": create_graph('countryName:"India" AND processUnitName:"Coker"',"India","Coker"),
            "japan cdu": create_graph('countryName:"Japan" AND processUnitName:"CDU"',"Japan","CDU"),
            "japan fcu": create_graph('countryName:"Japan" AND processUnitName:"FCU"',"Japan","FCU"),
            "japan coker": create_graph('countryName:"Japan" AND processUnitName:"Coker"',"Japan","Coker"),}
    return data


def do_jinga():
    data = get_jinja_dict()
    return ju.render_html(data, template='graphs.html', filename="test.html",
                          package_loader_name='world_refinery_database')

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


if __name__ == "__main__":
    get_capacity("Refinery/Country/Name eq 'United States' and Year le 2022 and ProcessUnitId eq 1 and CapacityStatusId in (1,2,4)")

# def getMarginsbyType(type: str):
#     Historical_data_URL = f"https://api.platts.com/odata/refinery-data/v2/Margins?&pageSize=1000&$expand=*"
#     df5 = pd.DataFrame()
#     while Historical_data_URL != "NaN":
#         time.sleep(1)  # api can only accept 2 requests per second and 5000 per day
#         data_request = requests.get(url=f"{Historical_data_URL}", headers=Headers_ref)
#         data = data_request.json()
#         df2 = pd.json_normalize(data).reset_index(drop=True)
#         x = df2["value"].iloc[0]
#         df3 = pd.json_normalize(x).reset_index(drop=True)
#         df3 = df3.drop_duplicates()
#         df5 = df5.append(df3, ignore_index=False)
#         try:
#             Historical_data_URL = df2[f"@odata.nextLink"].iloc[0]
#         except:
#             Historical_data_URL = "NaN"
#             continue
#     df5 = df5[df5["MarginType.Name"] == type]
#     df5 = df5.reset_index()
#     return df5
#
#
# def getCountryCapacityChangesTimeSeries(country: str):
#     Historical_data_URL = f"https://api.platts.com/odata/refinery-data/v2/capacity?$select=*&$expand=*&pageSize=1000&$filter=Refinery/Country/Name eq '{country}'"
#     df5 = pd.DataFrame()
#     while Historical_data_URL != "NaN":
#         time.sleep(1)  # api can only accept 2 requests per second and 5000 per day
#         data_request = requests.get(url=f"{Historical_data_URL}", headers=Headers_sup)
#         data = data_request.json()
#         df2 = pd.json_normalize(data).reset_index(drop=True)
#         x = df2["value"].iloc[0]
#         df3 = pd.json_normalize(x).reset_index(drop=True)
#         df3 = df3.drop_duplicates()
#         df3["Date"] = df3[["Year", "Quarter"]].apply(
#             lambda row: "-Q".join(row.values.astype(str)), axis=1
#         )
#         df4 = df3[["Refinery.Name", "Mbcd", "Mmtcd", "Mmtcy", "Date"]]
#         df4 = df4.groupby(["Date", "Refinery.Name"]).sum()
#         df4 = df4.reset_index()
#         df4["Date"] = pd.to_datetime(df4["Date"])
#         df5 = df5.append(df4, ignore_index=False)
#         try:
#             Historical_data_URL = df2[f"@odata.nextLink"].iloc[0]
#         except:
#             Historical_data_URL = "NaN"
#             continue
#     df4.columns = ["Date", "Refinery", "Mbdc", "Mmtcd", "Mmtcy"]
#     df5 = df4.groupby(["Date"]).sum()
#     df6 = df5.reset_index()
#     return df6
