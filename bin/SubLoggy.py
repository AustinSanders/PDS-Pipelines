#!/usgs/apps/anaconda/bin/python

from collections import OrderedDict

class SubLoggy(object):
"""
Parameters
----------
process

Methods
-------
__init__
setStatus
setCommand
setHelpLink
errorOut
getSLprocess
"""
    def __init__(self, process):
        self.process = process
        self.PDict = {}
        self.PDict[self.process] = OrderedDict()

    def setStatus(self, stat):
    """
    Parameters
    ----------
    stat
    """
        self.PDict[self.process]['status'] = stat 

    def setCommand(self, cmd):
    """
    Parameters
    ----------
    cmd
    """
        self.PDict[self.process]['command'] = cmd

    def setHelpLink(self, Hlink):
    """
    Parameters
    ----------
    Hlink
    """
        self.PDict[self.process]['helplink'] = Hlink

    def errorOut(self, error):
    """
    Parameters
    ----------
    error
    """
        self.PDict[self.process]['error'] = error

    def getSLprocess(self):
    """
    Returns
    ----------
    self.PDict
    """
        return self.PDict 


