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
from pds_pipelines.config import pds_db, cluster_db


class PDS_DBquery(object):
    def __init__(self, database):
        """
        Parameters
        -----------
        database : str
        """
        if database == "JOBS":
            self.session_maker, self.engine =  db_connect(cluster_db)
            Base = automap_base()
            Base.prepare(self.engine, reflect=True)
            self.processingTAB = Base.classes.processing
        elif database == "DI":
            base = automap_base()
            self.session_maker, _ =  db_connect(pds_db)
            self.files = Files
            self.archives = Archives
            self.DB_files = self.files

    def AddFile(self, Tfile):
        """
        Parameters
        ----------
        Tfile
        """
        session = self.session_maker()
        date = datetime.datetime.now(pytz.utc).strftime("%Y-%m-%d %H:%M:%S")

        insert = self.files(filename=str(Tfile),
                            entry_date=date)

        session.add(insert)
        session.commit()
        seesion.close()

    def jobKey(self):
        """
        Returns
        ------
        str
            key
        """
        session = self.session_maker()
        queryOBJ = session.query(self.processingTAB.key).filter(
            self.processingTAB.queued == None).order_by(self.processingTAB.submitted).first()
        session.close()
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
        session = self.session_maker()
        queryOBJ = session.query(self.processingTAB.xml).filter(
            self.processingTAB.key == key).first()
        session.close()
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
        session = self.session_maker()
        try:
            queryOBJ = session.query(self.processingTAB).filter(
                self.processingTAB.key == key).first()
            queryOBJ.queued = date
            session.commit()
            session.close()
            return date
        except AttributeError:
            session.close()
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
        session = self.session_maker()
        try:
            queryOBJ = session.query(self.processingTAB).filter(
                self.processingTAB.key == key).first()
            queryOBJ.started = date
            session.commit()
            session.close()
            return date
        except AttributeError:
            session.close()
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
        session = self.session_maker()
        try:
            queryOBJ = session.query(self.processingTAB).filter(
                self.processingTAB.key == key).first()
            queryOBJ.finished = date
            session.commit()
            session.close()
            return 'Success'
        except:
            session.close()
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
        session = self.session_maker()
        try:
            queryOBJ = session.query(self.processingTAB).filter(
                self.processingTAB.key == key).first()
            queryOBJ.error = errorxml
            session.commit()
            session.close()
            return 'Success'
        except:
            session.close()
            return 'Error'

