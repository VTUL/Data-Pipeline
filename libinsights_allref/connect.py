#!/usr/bin/env python
# local
import json
from datetime import date, timedelta
from csv import DictWriter as cwr

# foreign
import requests
from oauthlib.oauth2 import BackendApplicationClient
from requests_oauthlib import OAuth2Session
from requests.auth import HTTPBasicAuth
from enigma import get as eng

#import friendly


def first(rec, key):
    rec[key] = rec[key][0]
    return rec


def lowscore(astr, prefix='_'):
    return '%s%s' % (prefix,
                     '_'.join(astr.lower().split()))

def maplowscore(rec, key):
    return rec | {key: rec[lowscore(key)]}


def fmtrecords(recs):
    fmts = {'Question Type': first,
            'Start date': maplowscore}

    for k, _v in enumerate(recs):
        for _ak, _av in fmts.items(): recs[k]=_av(_v, _ak)

    return recs


def mkcsv(vals):
    flds = ['Question Type', 'Duration (in minutes)',
            'Comments/Notes', 'Location', 'who', 'mode',
            'answeredBy', 'classroom', 'walkins',
            'employee', 'studio', 'Start date']
    recs = fmtrecords(vals['payload']['records'])

    if len(recs) == 0:
        print(5, vals)
        return {
            'statusCode': 200,
            'body': 'empty set'
        }

    print(2, recs[0])
    acs = open('../ec-tide/brave.csv', 'w')
    acw = cwr(acs, flds, extrasaction='ignore')

    acw.writeheader()
    for _v in recs:
        acw.writerow(_v)
    acs.close()

    return {
        'statusCode': 200,
        'body': 'processed: %s  - total: %s - pages: %s' \
        % (len(recs), vals['payload']['total_records'],
           vals['payload']['total_pages'])
    }

def err_stat(grid, ret):
    return {
        'statusCode': 200,
        'body': grid + " - " \
        + str(ret.status_code) + " - " \
        + ret.reason
    }
    # from libinsights_allref.temp import da_ret
    # return mkcsv(da_ret)

def wkpast(adate, daysPast):
    return adate - \
        timedelta(days=adate.weekday() + daysPast)

def wksun(adate):
    #todo: investigate start week on sunday?
    return wkpast(adate, 8)

def wksat(adate):
    #todo: investigate start week on sunday?
    return wkpast(adate, 2)


def fromto(adate=date.today()):
    return {'from': wksun(adate),'to': wksat(adate)}

def gdata(token):
    grid = eng('grid_url')
    prms = {'request_id': '16', **fromto(),
            'entered_by': 'all',
            'show_notes': 'false', 'show_ip': 'false',
            'show_source': 'false', 'sort': 'asc',
            'page': '1'}
    headers = {'Authorization': token['token_type'] \
               + ' ' + token['access_token'],
               'Accept': 'application/json'}
    ret = requests \
        .get(grid, params=prms, headers=headers)

    if ret.status_code != 200: return err_stat(grid, ret)

    return mkcsv(ret.json())

def con():
    cid = eng('client_id')

    auth = HTTPBasicAuth(cid, eng('client_secret'))
    client = BackendApplicationClient(client_id=cid)
    oauth = OAuth2Session(client=client)
    token = oauth.fetch_token(token_url=eng('token_url'),
                              auth=auth)

    return gdata(token)


if __name__=='__main__':
    con()
