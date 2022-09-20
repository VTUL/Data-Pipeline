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
    grid = eng('grid_url') + '?'
    prms = ['request_id=16', 'from=2022-09-01',
            'to=2022-09-30', 'entered_by=all',
            'show_notes=false', 'show_ip=false',
            'show_source=false', 'sort=asc', 'page=1']
    for _v in prms:
        grid = grid + _v + '&'
    headers = {'Authorization': '{key}'.format(key=token)}
    ret = requests.get(grid,headers=headers).json()

    return {
        'statusCode': 200,
        'body': json.dumps(ret)
    }


if __name__=='__main__':
    con()
