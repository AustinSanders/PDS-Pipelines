#!/usgs/apps/anaconda/bin/python

import os
import sys
import datetime
import pytz

from PDS_DBsessions import *


class PDS_DBquery(PDS_DBsessions):
"""
Parameters
----------
PDS_DBsessions
"""
    def jobKey(self):

        queryOBJ = self.session.query(self.processingTAB.key).filter(
            self.processingTAB.queued == None).order_by(self.processingTAB.submitted).first()

        for key in queryOBJ:
            return key

    def jobXML4Key(self, key):
    """
    Parameters
    ----------
    key


    Returns
    -------
    queryOBJ.xml
    """
        queryOBJ = self.session.query(self.processingTAB.xml).filter(
            self.processingTAB.key == key).first()
        return queryOBJ.xml

    def setJobsQueued(self, inkey):
    """
    Parameters
    ----------
    inkey

    Returns
    -------
    Success
    Error
    """
        date = datetime.datetime.now(pytz.utc).strftime("%Y-%m-%d %H:%M:%S.%f")
        try:
            queryOBJ = self.session.query(self.processingTAB).filter(
                self.processingTAB.key == inkey).one()
            queryOBJ.queued = date
            self.session.commit()
            return 'Success'
        except:
            return 'Error'

    def setJobsStarted(self, inkey):
    """
    Parameters
    ----------
    inkey

    Returns
    -------
    Success
    Error
    """
        date = datetime.datetime.now(pytz.utc).strftime("%Y-%m-%d %H:%M:%S.%f")
        try:
            queryOBJ = self.session.query(self.processingTAB).filter(
                self.processingTAB.key == inkey).one()
            queryOBJ.started = date
            self.session.commit()
            return 'Success'
        except:
            return 'Error'

    def setJobsFinished(self, inkey):
    """
    Parameters
    ----------
    inkey

    Reurns
    ------
    Success
    Error
    """
        date = datetime.datetime.now(pytz.utc).strftime("%Y-%m-%d %H:%M:%S.%f")
        try:
            queryOBJ = self.session.query(self.processingTAB).filter(
                self.processingTAB.key == inkey).one()
            queryOBJ.finished = date
            self.session.commit()
            return 'Success'
        except:
            return 'Error'

    def addErrors(self, key, errorxml):
    """
    Parameters
    ----------
    key
    errorxml

    Returns
    -------
    Success : str
    Error : str
    """
        try:
            queryOBJ = self.session.query(self.processingTAB).filter(
                self.processingTAB.key == key).one()
            queryOBJ.error = errorxml
            self.session.commit()
            return 'Success'
        except:
            return 'Error'
