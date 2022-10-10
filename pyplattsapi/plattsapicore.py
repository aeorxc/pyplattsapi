import os
from functools import lru_cache
from cachetools.func import ttl_cache

import requests

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
