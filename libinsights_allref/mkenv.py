#!/usr/bin/env python
# local
import json, os
import labrynth as lbr

# foreign
# from enigma import get as eng

if __name__=='__main__':
    # ['', 'host', 'token_url']
    # client_id
    # client_secret
    # host
    # token_url = host + 'oauth/token'
    # grid_url = host + 'custom-dataset/28364/data-grid?'
    vrs="'Variables={"
    vrs = vrs + 'DP_ENV="dev",'
    for _v in dir(lbr):
        if _v[0:2] == '__': continue
        vrs = vrs + _v + '="' + getattr(lbr, _v) +'",'

    print(1, "aws lambda update-function-configuration" \
              + " --function-name doom-aleph" \
              + " --environment " + vrs[:-1] + "}'")
    os.system("aws lambda update-function-configuration" \
              + " --function-name doom-aleph" \
              + " --environment " + vrs[:-1] + "}'")

# aws lambda update-function-configuration --function-name doom-aleph --environment 'Variables={host="https://vt.libinsight.com/v 1.0/",token_url="oauth/token"}'
