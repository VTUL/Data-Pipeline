#!/usr/bin/env python
# local
import os,sys,json,re
from os import system

# foreign
from oauthlib.oauth2 import BackendApplicationClient
from requests_oauthlib import OAuth2Session
from requests.auth import HTTPBasicAuth

#import friendly

def con():
    from enigma import get as eng 
    cid = eng('client_id')

    auth = HTTPBasicAuth(cid, eng('client_secret'))
    client = BackendApplicationClient(client_id=cid)
    oauth = OAuth2Session(client=client)
    token = oauth.fetch_token(token_url=eng('token_url'),
                              auth=auth)

    return {
        'statusCode': 200,
        'body': json.dumps(token)
    }
    print(1, token)

if __name__=='__main__':
    con()
