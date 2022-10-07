import os
import requests

def getCredentials():
    SUP_APP_Key = {'appkey': os.getenv('SUPPLY_API_KEY')}
    REF_APP_Key = {'appkey': os.getenv('REFINERY_API_KEY')}
    Cred_Data = {'username': os.getenv('Cred_Data_Username'), 'password': os.getenv('Cred_Data_Password')}
    token_request_sup = requests.post("https://api.platts.com/auth/api", headers=SUP_APP_Key, data=Cred_Data)
    req_dic_sup = token_request_sup.json()
    access_token_sup = req_dic_sup['access_token']
    Bearer_Token_sup = "Bearer " + access_token_sup
    Headers_sup = {'accept': 'application/json', 'appkey': os.getenv('SUPPLY_API_KEY'), 'Authorization': Bearer_Token_sup}
    token_request_ref = requests.post("https://api.platts.com/auth/api", headers=REF_APP_Key, data=Cred_Data)
    req_dic_ref = token_request_ref.json()
    access_token_ref = req_dic_ref['access_token']
    Bearer_Token_ref = "Bearer " + access_token_ref
    Headers_ref = {'accept': 'application/json', 'appkey': os.getenv('REFINERY_API_KEY'), 'Authorization': Bearer_Token_ref}
    Headers_inv = {'accept': 'application/json', 'appkey': os.getenv('INVENTORY_API_KEY')}
    return Headers_sup, Headers_ref, Headers_inv