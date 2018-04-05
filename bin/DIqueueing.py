#!/usgs/apps/anaconda/bin/python

import os, sys, subprocess, datetime, pytz

import logging
import argparse
import json

from RedisQueue import *
from HPCjob import *

from sqlalchemy import Date, cast

import sqlalchemy
from sqlalchemy import *
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm import mapper
from sqlalchemy import create_engine
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm.util import *
from sqlalchemy.ext.declarative import declarative_base
from db import Files, Archives, db_connect

from sqlalchemy import or_
import pdb

class Args:
    """
    Creates an object 'parser', which is populated by the attributes 
    'archive', 'volume', and 'jobarray'. 

    Attributes
    ----------
    archive : str
    volume : str
    jobarray : str

    Methods
    -------
    _init_(self)
        constructor
    parse_args(self)
        Converts argument strings into objects that are attributes
        of the namespace
    """ 
    def __init__(self):
        pass

    def parse_args(self):
        """
        Defines how to parse single command-line argument

        Parameters
        ----------
        name : str 
              '--archive'

        dest : str
            name of attribute to be added to parser
        
        required : str
            makes optional arguments required

        help
            contains brief description of argument

        Returns
        -------
        args : 
            object with attributes 'archive', 'volume', and 'jobarray'
        """
        
        parser = argparse.ArgumentParser(description='PDS DI Data Integrity')
        
        parser.add_argument('--archive', '-a', dest="archive", required=True,
                          help="Enter archive - archive to ingest")

        parser.add_argument('--volume', '-v', dest="volume",
                          help="Enter volume to Ingest")

        parser.add_argument('--jobarray', '-j', dest="jobarray",
                          help="Enter string to set job array size")

        args = parser.parse_args()

        self.archive = args.archive
        self.volume = args.volume
        self.jobarray = args.jobarray


def main():

    pdb.set_trace()
    args = Args()
    args.parse_args()

    RQ = RedisQueue('DI_ReadyQueue')

    PDSinfoDICT = json.load(open('/usgs/cdev/PDS/bin/PDSinfo.json', 'r'))
    archiveID = PDSinfoDICT[args.archive]['archiveid']

##********* Set up logging *************
    logger = logging.getLogger('DI_Queueing.' + args.archive)
    logger.setLevel(logging.INFO)
    logFileHandle = logging.FileHandler('/usgs/cdev/PDS/logs/DI.log')
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s, %(message)s')
    logFileHandle.setFormatter(formatter)
    logger.addHandler(logFileHandle)

    logger.info('Starting %s DI Queueing', args.archive)
    if args.volume:
        logger.info('Queueing %s Volume', args.volume) 


    try:
        # Throws away engine information
        session, files, archives, _ = db_connect('pdsdi')
        logger.info('DataBase Connecton: Success')
    except:
        logger.error('DataBase Connection: Error')

# ************* date stuff ***************

    td = (datetime.datetime.now(pytz.utc) - datetime.timedelta(days=30)).strftime("%Y-%m-%d %H:%M:%S")
    testing_date = datetime.datetime.strptime(str(td), "%Y-%m-%d %H:%M:%S")

    if args.volume:
        volstr = '%' + args.volume + '%'
        testcount = session.query(Files).filter(
            files.c.archiveid == archiveID, files.c.filename.like(volstr)).filter(
                or_(cast(files.c.di_date, Date) < testing_date,
                    cast(files.c.di_date, Date) == None)).count()
#        logger.info('Query Count %s', testcount) 
        testQ = session.query(Files).filter(
            files.c.archiveid == archiveID, files.c.filename.like(volstr)).filter(
                or_(cast(files.c.di_date, Date) < testing_date,
                    cast(files.c.di_date, Date) == None))
    else:
        testcount = session.query(Files).filter(files.c.archiveid == archiveID).filter(
            or_(cast(files.c.di_date, Date) < testing_date,
                cast(files.c.di_date, Date) == None)).count()
        testQ = session.query(Files).filter(files.c.archiveid == archiveID).filter(
            or_(cast(files.c.di_date, Date) < testing_date,
                cast(files.c.di_date, Date) == None))
        
    addcount = 0
    for element in testQ:
        try:
            RQ.QueueAdd(element.filename)
            addcount = addcount + 1
        except:
            logger.error('File %s Not Added to DI_ReadyQueue', element.filename)
    
    logger.info('Files Added to Queue %s', addcount)        

    logger.info('DI Queueing Complete')
    
if __name__ == "__main__":
    sys.exit(main())

