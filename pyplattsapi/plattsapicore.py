import os
import time
from functools import lru_cache
from urllib.error import HTTPError

import pandas as pd
import requests
from cachetools.func import ttl_cache

api_url = "https://api.platts.com"
auth_url = f"{api_url}/auth/api"


@lru_cache(maxsize=20)
def extract_credential(key: str):
    credentials = os.getenv("PLATTS_API_CREDENTIALS")
    tokens = credentials.split(";")
    token = [x for x in tokens if x.startswith(key)]
    if len(token) > 0:
        s = token[0].split("=")
        return s[1]


def get_access_token(api_dataset):
    """
    Given an api_dataset (eg OIL INVENTORY), get an access token from the Auth API
    :param api_dataset:
    :return:
    """
    data = {
        "username": extract_credential("username"),
        "password": extract_credential("password"),
    }
    headers = {"appkey": extract_credential(api_dataset)}
    token_request_sup = requests.post(auth_url, headers=headers, data=data)
    req_dic_sup = token_request_sup.json()
    access_token_sup = req_dic_sup["access_token"]
    return access_token_sup


@ttl_cache(ttl=10 * 60)
def build_header(api_dataset):
    header = {
        "accept": "application/json",
        "appkey": extract_credential(api_dataset),
        "Authorization": f"Bearer {get_access_token(api_dataset)}",
    }
    return header


def generic_api_call_helper(api: str, api_name: str, params: dict, page: int = 1):
    params['page'] = page
    response = requests.get(
        url=api, headers=build_header(api_name), params=params
    )
    if response.status_code == 200:
        return response.json()
    else:
        raise HTTPError(url=api, code=response.status_code, msg=response.json()['cause'], hdrs=None, fp=None)


def generic_api_call(api: str, api_name: str, params: dict):
    # make call for page 1
    r = generic_api_call_helper(api, api_name, params)
    max_page = int(r["metadata"]["count"] / 1000)
    res = pd.concat([pd.Series(x) for x in r["results"]], axis=1)
    # make call for pages 2 -> n (if needed as determined by max_page)
    for page in range(2, max_page + 2):
        time.sleep(0.55) # 2 calls per second allowed
        r = generic_api_call_helper(api, api_name, params, page=page)
        d = pd.concat([pd.Series(x) for x in r["results"]], axis=1)
        res = pd.concat([d, res], axis=1)

    res = res.T
    return res
