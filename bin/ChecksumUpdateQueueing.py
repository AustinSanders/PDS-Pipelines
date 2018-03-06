#!/usgs/apps/anaconda/bin/python

import os, sys, subprocess, datetime, pytz

import logging
import argparse

from RedisQueue import *
from HPCjob import *

import sqlalchemy
from sqlalchemy import *
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm import mapper
from sqlalchemy import create_engine
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm.util import *
from sqlalchemy.ext.declarative import declarative_base

import pdb

class Args:
    def __init__(self):
        pass

    def parse_args(self):

        parser = argparse.ArgumentParser(description='PDS DI Data Integrity')

        parser.add_argument('--archive', '-a', dest="archive", required=True,
                          help="Enter archive - archive to ingest")

        parser.add_argument('--volume', '-v', dest="volume",
                          help="Enter voluem to Ingest")

        args = parser.parse_args()

        self.archive = args.archive
        self.volume = args.volume

def main():

#    pdb.set_trace()

    args = Args()
    args.parse_args()

    RQ = RedisQueue('ChecksumUpdate_Queue')

    archiveDICT = {'cassiniISS': '/pds_san/PDS_Archive/Cassini/ISS',
                   'mroCTX': '/pds_san/PDS_Archive/Mars_Reconnaissance_Orbiter/CTX',
                   'mroHIRISE': '/pds_san/PDS_Archive/Mars_Reconnaissance_Orbiter/HiRISE',
                   'LROLRC_EDR': '/pds_san/PDS_Archive/Lunar_Reconnaissance_Orbiter/LROC/EDR/'
                  }

    archiveID = {'cassiniISS': 'cassini_iss_edr',
                'mroCTX': 16,
                'mroHIRISE_EDR': '124',
                'LROLRC_EDR': 74
               }


##********* Set up logging *************
    logger = logging.getLogger('ChecksumUpdate_Queueing.' + args.archive)
    logger.setLevel(logging.INFO)
    logFileHandle = logging.FileHandler('/usgs/cdev/PDS/logs/DI.log')
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s, %(message)s')
    logFileHandle.setFormatter(formatter)
    logger.addHandler(logFileHandle)

    logger.info('Starting %s Checksum update Queueing', args.archive)
    if args.volume:
        logger.info('Queueing %s Volume', args.volume) 


    try:
        engine = create_engine('postgresql://pdsdi:dataInt@dino.wr.usgs.gov:3309/pds_di_prd')
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
        logger.info('DataBase Connecton: Success')
    except:
        logger.error('DataBase Connection: Error')


    if args.volume:
        volstr = '%' + args.volume + '%'
        QueryOBJ = session.query(Files).filter(files.c.archiveid == archiveID[args.archive], files.c.filename.like(volstr))
    else:
        QueryOBJ = session.query(Files).filter(files.c.archiveid == archiveID[args.archive])
    addcount = 0
    for element in QueryOBJ:
        try:
            RQ.QueueAdd(element.filename)
            addcount = addcount + 1
        except:
            logger.error('File %s Not Added to DI_ReadyQueue', element.filename)

    
    logger.info('Files Added to Queue %s', addcount)        

    logger.info('DI Queueing Complete')
    
if __name__ == "__main__":
    sys.exit(main())
