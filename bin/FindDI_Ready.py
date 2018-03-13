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

def main():

    args = Args()
    args.parse_args()

    PDSinfoDICT = json.load(open('/usgs/cdev/PDS/bin/PDSinfo.json', 'r'))
    archiveID = PDSinfoDICT[args.archive]['archiveid']

    try:
        engine = create_engine('postgresql://{}:{}@{}:{}/{}'.format(pdsdi_user,
                                                                    pdsdi_pass,
                                                                    pdsdi_host,
                                                                    pdsdi_port,
                                                                    pdsdi_db))

        metadata = MetaData(bind=engine)
        files = Table('files', metadata, autoload=True)
        archives = Table('archives', metadata, autoload=True)

        class Files(object):
            pass
        class Archives(object):
            pass

        filesmapper = mapper(Files, files)
        archivesmapper = mapper(Archives, archives)
        Session = sessionmaker()
        session = Session()
        print 'Database Connection Success'
    except:
        print 'Database Connection Error'

    td = (datetime.datetime.now(pytz.utc) - datetime.timedelta(days=30)).strftime("%Y-%m-%d %H:%M:%S")
    testing_date = datetime.datetime.strptime(str(td), "%Y-%m-%d %H:%M:%S")

    if args.volume:
        volstr = '%' + args.volume + '%'
        testQ = session.query(Files).filter(files.c.archiveid == archiveID,
                                            files.c.filename.like(volstr)).filter(or_(cast(files.c.di_date, Date) < testing_date,
                                                                                      cast(files.c.di_date, Date) == None)).count()
        if testQ > 0:
            print 'Volume %s DI Ready: %s Files' % (args.volume, str(testQ))
        elif testQ == 0:
            print 'Volume %s DI Current' % args.volume

    else:
        testQ = session.query(Files).filter(files.c.archiveid == archiveID).filter(or_(cast(files.c.di_date, Date) < testing_date,
                                                                                    cast(files.c.di_date, Date) == None)).count()
        if testQ > 0:
            print 'Archive %s DI Ready: %s Files' % (args.archive, str(testQ))
        elif testQ == 0:
            print 'Archive %s DI Current' % args.archive

    

if __name__ == "__main__":
    sys.exit(main())
