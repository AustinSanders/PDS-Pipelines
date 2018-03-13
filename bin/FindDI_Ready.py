#!/usgs/apps/anaconda/bin/python

import os, sys, subprocess, datetime, pytz
import argparse
import json

import sqlalchemy

from sqlalchemy import Date, cast

from sqlalchemy import *
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm import mapper
from sqlalchemy import create_engine
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm.util import *
from sqlalchemy.ext.declarative import declarative_base

import pdb
from config import *

class Args:
    def __init__(self):
        pass

    def parse_args(self):
        parser = argparse.ArgumentParser(description='Find Archives for DI')

        parser.add_argument('--archive', '-a', dest="archive",
                          help="Enter archive to test for DI")

        parser.add_argument('--volume', '-v', dest="volume",
                          help="Enter Volume to Test")

        args = parser.parse_args()
        self.archive = args.archive
        self.volume = args.volume


class Files(object):
    pass


class Archives(object):
    pass


def db_connect():
    engine = create_engine('postgresql://{}:{}@{}:{}/{}'.format(pdsdi_user,
                                                            pdsdi_pass,
                                                            pdsdi_host,
                                                            pdsdi_port,
                                                            pdsdi_db))

    metadata = MetaData(bind=engine)
    files = Table('files', metadata, autoload=True)
    archives = Table('archives', metadata, autoload=True)
    filesmapper = mapper(Files, files)
    archivesmapper = mapper(Archives, archives)
    Session = sessionmaker()
    session = Session()

    return session, files, archives


def archive_expired(session, archiveID, files, testing_date = None):
    if testing_date == None:
        td = (datetime.datetime.now(pytz.utc) - datetime.timedelta(days=30)).strftime("%Y-%m-%d %H:%M:%S")
        testing_date = datetime.datetime.strptime(str(td), "%Y-%m-%d %H:%M:%S")

    testQ = session.query(Files).filter(files.c.archiveid == archiveID).filter(or_(cast(files.c.di_date, Date) < testing_date,
                                                                                cast(files.c.di_date, Date) == None)).count()
    return testQ

def volume_expired(session, archiveID, files, testing_date = None):
    if testing_date == None:
        td = (datetime.datetime.now(pytz.utc) - datetime.timedelta(days=30)).strftime("%Y-%m-%d %H:%M:%S")
        testing_date = datetime.datetime.strptime(str(td), "%Y-%m-%d %H:%M:%S")

    volstr = '%' + args.volume + '%'
    testQ = session.query(Files).filter(files.c.archiveid == archiveID,
                                        files.c.filename.like(volstr)).filter(or_(cast(files.c.di_date, Date) < testing_date,
                                                                                cast(files.c.di_date, Date) == None)).count()

    return testQ

def main():

    args = Args()
    args.parse_args()

    PDSinfoDICT = json.load(open('/usgs/cdev/PDS/bin/PDSinfo.json', 'r'))
    archiveID = PDSinfoDICT[args.archive]['archiveid']

    try:
        session, files, archives = db_connect()
        print(args.archive)
        print('Database Connection Success')
    except Exception as e:
        print(e)
    else:
        # if db connection fails, there's no sense in doing this part
        td = (datetime.datetime.now(pytz.utc) - datetime.timedelta(days=30)).strftime("%Y-%m-%d %H:%M:%S")
        testing_date = datetime.datetime.strptime(str(td), "%Y-%m-%d %H:%M:%S")

        if args.volume:
            if volume_expired(session, archiveID, files, testing_date):
                print('Volume {} DI Ready: {} Files'.format(args.volume, str(testQ)))
            else:
                print('Volume {} DI Curent'.format(args.volume))
        else:
            if archive_expired(session, archiveID, files, testing_date):
                print('Archive {} DI Ready: {} Files'.format(args.archive, str(testQ)))
            else:
                print('Archive {} DI Curent'.format(args.archive))
                

if __name__ == "__main__":
    sys.exit(main())
