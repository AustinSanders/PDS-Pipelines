#!/usgs/apps/anaconda/bin/python

import os, subprocess, sys, datetime

import sqlalchemy
from sqlalchemy import *
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm.util import *
from sqlalchemy.orm import eagerload


class UPC_test(object):

    def __init__(self):

        Base = declarative_base()
        engine = create_engine('postgresql://upcmgr:un1pl@c0@dino.wr.usgs.gov:3309/upc_dev')
        metadata = MetaData(bind=engine)
        Session = sessionmaker(bind=engine)
        self.session = Session

        Base = automap_base()
        Base.prepare(engine, reflect=True)

        self.DBT_keywords = Base.classes.keywords

    def get_keyword_typeid(self, key):

        Qobj = self.session.query(self.DBT_keywords).filter(self.DBT_keywords.typename == key).first()
        return Qobj.typeid
