#!/usgs/apps/anaconda/bin/python

import json
from collections import OrderedDict

from SubLoggy import *

class Loggy(object):
"""
Parameters
----------
object
Methods
-------
__init__
setFileStatus
addProcess
Loggy2json
"""
    def __init__(self, Lfile):
        """
        Parameters
        ----------
        Lfile
        """"
        self.Lfile = Lfile
        self.loggyDict = {}
        self.loggyDict[self.Lfile] = OrderedDict()


    def setFileStatus(self, item):
        """
        Parameters
        ----------
        item
        """"
        pass


    def AddProcess(self, PD):
        """
        Parameters
        ----------
        PD
        """"
        self.loggyDict[self.Lfile].update(PD)

    def Loggy2json(self):
        """
        Returns
        ----------
        Ljson
        """"
        Ljson = json.dumps(self.loggyDict)
        return Ljson
