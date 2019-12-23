#!/usr/bin/env python

import os
import sys
import datetime
import pytz
import subprocess

import sqlalchemy
from sqlalchemy import *
from sqlalchemy.orm.util import *
from sqlalchemy.ext.automap import automap_base

from pds_pipelines.db import db_connect
from pds_pipelines.models.pds_models import Files, Archives
from pds_pipelines.config import pds_db


class PDS_DBquery(object):
    def __init__(self, database):
        """
        Parameters
        -----------
        database : str
        """
        if database == "JOBS":
            self.session, self.engine =  db_connect('clusterjob_prd')
            DBsession = self.session
            Base = automap_base()
            Base.prepare(self.engine, reflect=True)
            self.processingTAB = Base.classes.processing
        elif database == "DI":
            base = automap_base()
            self.session, _ =  db_connect(pds_db)
            self.files = Files
            self.archives = Archives
            DBsession = self.session
            self.DB_files = self.files


    def closeDB(self):

        self.session.close()

    def AddFile(self, Tfile):
        """
        Parameters
        ----------
        Tfile
        """
        date = datetime.datetime.now(pytz.utc).strftime("%Y-%m-%d %H:%M:%S")

        insert = self.files(filename=str(Tfile),
                            entry_date=date)

        self.session.add(insert)
        self.session.commit()

    def jobKey(self):
        """
        Returns
        ------
        str
            key
        """

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
        xml
            queryOBJ.xml
        """
        queryOBJ = self.session.query(self.processingTAB.xml).filter(
            self.processingTAB.key == key).first()
        try:
            return queryOBJ.xml
        except AttributeError:
            raise KeyError("Key {} not found in jobs database".format(key))


    def setJobsQueued(self, key):
        """
        Parameters
        ----------
        key

        Returns
        -------
        str
            Success is succesful, Error otherwise
        """
        date = datetime.datetime.now(pytz.utc).strftime("%Y-%m-%d %H:%M:%S.%f")
        try:
            queryOBJ = self.session.query(self.processingTAB).filter(
                self.processingTAB.key == key).first()
            queryOBJ.queued = date
            self.session.commit()
            return date
        except AttributeError:
            raise KeyError("Key {} not found in jobs database".format(key))

    def setJobsStarted(self, key):
        """
        Parameters
        ----------
        key

        Returns
        -------
        str
            Success is succesful, Error otherwise
        """
        date = datetime.datetime.now(pytz.utc).strftime("%Y-%m-%d %H:%M:%S.%f")
        try:
            queryOBJ = self.session.query(self.processingTAB).filter(
                self.processingTAB.key == key).first()
            queryOBJ.started = date
            self.session.commit()
            return date
        except AttributeError:
            raise KeyError("Key {} not found in jobs database".format(key))

    def setJobsFinished(self, key):
        """
        Parameters
        ----------
        key

        Reurns
        ------
        str
            Success is succesful, Error otherwise
        """
        date = datetime.datetime.now(pytz.utc).strftime("%Y-%m-%d %H:%M:%S.%f")
        try:
            queryOBJ = self.session.query(self.processingTAB).filter(
                self.processingTAB.key == key).first()
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
        str
            Success is succesful, Error otherwise
        """
        queryOBJ = self.session.query(self.processingTAB).filter(
            self.processingTAB.key == key).first()
        queryOBJ.error = errorxml
        self.session.commit()
