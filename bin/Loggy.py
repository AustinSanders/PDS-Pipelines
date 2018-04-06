#!/usgs/apps/anaconda/bin/python

import json
from collections import OrderedDict

from SubLoggy import *


class Loggy(object):

    def __init__(self, Lfile):

        self.Lfile = Lfile
        self.loggyDict = {}
        self.loggyDict[self.Lfile] = OrderedDict()

    def setFileStatus(self, item):
        pass

    def AddProcess(self, PD):
        self.loggyDict[self.Lfile].update(PD)

    def Loggy2json(self):
        Ljson = json.dumps(self.loggyDict)
        return Ljson
