#!/usgs/apps/anaconda/bin/python

import os, subprocess, sys, datetime, pytz

import sqlalchemy
from sqlalchemy import *
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm import mapper
from sqlalchemy import create_engine
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm.util import *
from sqlalchemy.orm import eagerload

from sqlalchemy.ext.declarative import declarative_base

from config import *

class PDS_DBsessions(object):

    def __init__(self, database):

        if database == "JOBS":

            engine = create_engine('postgresql://{}:{}@{}:{}/{}'.format(clusterjobs_user
                                                                        clusterjobs_pass
                                                                        clusterjobs_host
                                                                        clusterjobs_port
                                                                        clusterjobs_db))


            Session = sessionmaker(bind=engine)
            self.session = Session()
            DBsession = self.session

            Base = automap_base()
            Base.prepare(engine, reflect=True)

            self.processingTAB = Base.classes.processing

        elif database == "DI":

            base = automap_base()

            engine = create_engine('postgresql://{}:{}@{}:{}/{}'.format(pdsdi_user
                                                                        pdsdi_pass
                                                                        pdsdi_host
                                                                        pdsdi_port
                                                                        pdsdi_db))

 
            base.prepare(engine, reflect=True)

            self.files = base.classes.files
            self.archives = base.classes.archives
       
            Session = sessionmaker(bind=engine)
            self.session = Session()
            DBsession = self.session

            self.DB_files = self.files

#        elif database == "UPC":
 
#            base = automap_base()      
#            engine = create_engine('postgresql://upcmgr:un1pl@c0@dino.wr.usgs.gov:3309/upc_dev')
#            engine = create_engine('postgresql://upcmgr:un1pl@c0@dino.wr.usgs.gov:3309/upc_prd')

#            mymetadata = MetaData()
#            print mymetadata.tables

#            mymetadata = MetaData(engine, reflect=True) 
#            print mymetadata.tables          
           
#            filesmapper = mapper(
#            Session = sessionmaker(bind=engine)
#            self.DBsession = Session()

#            metadata = MetaData(bind=engine)
#            datafiles = Table('datafiles', metadata, autoload=True)
#            class Datafiles(object):
#                pass
# 
#            filesmapper = mapper(Datafiles, datafiles)
#            Session = sessionmaker()
#            self.DBsession = Session()

#            base.prepare(engine, reflect=True)
#
#            self.datafilesTAB = base.classes.datafiles
#            self.targetsTAB = base.classes.targets_meta
#            self.instrumentsmetaTAB = base.classes.instrumentsmeta
#
#            Session = sessionmaker(bind=engine)
#            self.session = Session()
#            DBsession = self.session


    def closeDB(self):
    
        self.session.close()

    def AddFile(self, Tfile):
        date = datetime.datetime.now(pytz.utc).strftime("%Y-%m-%d %H:%M:%S")

        insert = self.files(filename = str(Tfile),
                            entry_date = date)

        self.session.add(insert)
        self.session.commit()
