#!/usr/bin/env python
# local
import os,sys,json,re
from os import system

# foreign
from oauthlib.oauth2 import BackendApplicationClient
from requests_oauthlib import OAuth2Session
from requests.auth import HTTPBasicAuth

#import friendly

if __name__=='__main__':

    from labrynth import client_id, client_secret, token_url

    auth = HTTPBasicAuth(client_id, client_secret)
    client = BackendApplicationClient(client_id=client_id)
    oauth = OAuth2Session(client=client)
    token = oauth.fetch_token(token_url=token_url, auth=auth)

    print(1, token)
