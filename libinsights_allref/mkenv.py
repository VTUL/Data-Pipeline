#!/usr/bin/env python
# local
import json, os
import labrynth as lbr

# foreign
# from enigma import get as eng

if __name__=='__main__':
    vrs="'Variables={"
    vrs = vrs + 'DP_ENV="dev",'
    for _v in dir(lbr):
        if _v[0:2] == '__': continue
        vrs = vrs + _v + '="' + getattr(lbr, _v) +'",'

    os.system("aws lambda update-function-configuration" \
              + " --function-name doom-aleph" \
              + " --environment " + vrs[:-1] + "}'")

# aws lambda update-function-configuration --function-name doom-aleph --environment 'Variables={host="https://vt.libinsight.com/v 1.0/",token_url="oauth/token"}'
