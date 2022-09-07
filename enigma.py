"""wrapper for environment/file config"""
import os

def get(valname):
    """get value or None based on DP_ENV"""
    cnv = 'DP_ENV'
    env = os.environ.get(cnv, None)
    tst = 'testloc'
    if env != tst and env is not None:
        return os.environ.get(valname, None)

    import libinsights_allref.labrynth as lbr
    return getattr(lbr, valname)
