#!/usgs/apps/anaconda/bin/python

import os
import sys
import pvl


def find_keyword(obj, key):
    if key is None or obj is None:
        return None
    elif key in obj:
        return obj[key]
    for k, v in obj.items():
        if isinstance(v, dict):
            F_item = find_keyword(v, key)
            if F_item is not None:
                return F_item
            
def lower_keys(x):
    if isinstance(x, list):
        return [lower_keys(v) for v in x]
    elif isinstance(x, dict):
        return dict((k.lower(), lower_keys(v)) for k, v in x.items())
    else:
        return x

class UPCkeywords(object):

    def __init__(self, pvlfile):
        self.label = lower_keys(pvl.load(pvlfile))

    def getKeyword(self, keyword):
        Gkey = find_keyword(self.label, keyword.lower())
        return Gkey

    def getPolygonKeyword(self, keyword):

        get1 = find_keyword(self.label, 'Polygon')
        Gkey = find_keyword(get1, keyword)
        if keyword == 'CentroidRadius':
            Gkey = Gkey[0]
        return Gkey
