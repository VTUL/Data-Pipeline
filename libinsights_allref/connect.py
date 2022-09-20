#!/usr/bin/env python
# local
import os,sys,json,re
from os import system

# foreign
import requests
from oauthlib.oauth2 import BackendApplicationClient
from requests_oauthlib import OAuth2Session
from requests.auth import HTTPBasicAuth
from enigma import get as eng

#import friendly

def con():
    cid = eng('client_id')

    auth = HTTPBasicAuth(cid, eng('client_secret'))
    client = BackendApplicationClient(client_id=cid)
    oauth = OAuth2Session(client=client)
    token = oauth.fetch_token(token_url=eng('token_url'),
                              auth=auth)

    return gdata(token)
    print(0, 'script-doom')
    return {
        'statusCode': 200,
        'body': json.dumps(token)
    }

def gdata(token):
    grid = eng('grid_url')
    prms = {'request_id': '16', 'from': '2022-09-01',
            'to': '2022-09-01', 'entered_by': 'all',
            'show_notes': 'false', 'show_ip': 'false',
            'show_source': 'false', 'sort': 'asc',
            'page': '1'}
    # headers = {'Authorization': token['access_token']}
    headers = {'Authorization': token['token_type'] \
               + ' ' + token['access_token'],
               'Accept': 'application/json'}
    ret = requests \
        .get(grid, params=prms, headers=headers) \
        .json()
    # ret = requests.get(grid,headers=headers).json()

    # print(1, ret.status_code, ret.text, dir(ret.request), ret.request.url, ret.request.headers)
    return {
        'statusCode': 200,
        'body': json.dumps(ret)
    }


if __name__=='__main__':
    con()
