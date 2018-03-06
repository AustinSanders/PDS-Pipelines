#!/usgs/apps/anaconda/bin/python

from collections import OrderedDict

class SubLoggy(object):

    def __init__(self, process):
       
        self.process = process
        self.PDict = {}
        self.PDict[self.process] = OrderedDict()

    def setStatus(self, stat):

        self.PDict[self.process]['status'] = stat 

    def setCommand(self, cmd):

        self.PDict[self.process]['command'] = cmd

    def setHelpLink(self, Hlink):

        self.PDict[self.process]['helplink'] = Hlink

    def errorOut(self, error):

        self.PDict[self.process]['error'] = error

    def getSLprocess(self):
        return self.PDict 


