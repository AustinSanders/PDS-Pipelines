#!/usgs/apps/anaconda/bin/python

import os
import sys
import subprocess
import datetime
import pytz
import json
import logging
import argparse

from pds_pipelines.RedisQueue import *
from pds_pipelines.HPCjob import *

import pdb


class Args:
    """
    Attributes
    ----------
    archive : str
    volume : str
    search : str
    """
    def __init__(self):
        pass

    def parse_args(self):

        parser = argparse.ArgumentParser(description='PDS DI Database Ingest')

        parser.add_argument('--archive', '-a', dest="archive", required=True,
                            help="Enter archive - archive to ingest")

        parser.add_argument('--volume', '-v', dest="volume",
                            help="Enter voluem to Ingest")

        parser.add_argument('--search', '-s', dest="search",
                            help="Enter string to search for")

        parser.add_argument('--link-only', dest='ingest', action='store_false')
        parser.set_defaults(ingest=True)
        args = parser.parse_args()

        self.archive = args.archive
        self.volume = args.volume
        self.search = args.search
        self.ingest = args.ingest

def main():

    # pdb.set_trace()

    args = Args()
    args.parse_args()

    RQ_ingest = RedisQueue('Ingest_ReadyQueue')
    RQ_linking = RedisQueue('LinkQueue')

# ********* Set up logging *************
    logger = logging.getLogger(args.archive + '_INGEST')
    logger.setLevel(logging.INFO)
    #logFileHandle = logging.FileHandler('/usgs/cdev/PDS/logs/Ingest.log')
    logFileHandle = logging.FileHandler('/home/arsanders/PDS-Pipelines/logs/Ingest.log')
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s, %(message)s')
    logFileHandle.setFormatter(formatter)
    logger.addHandler(logFileHandle)

    PDSinfoDICT = json.load(open('/usgs/cdev/PDS/bin/PDSinfo.json', 'r'))
    archivepath = PDSinfoDICT[args.archive]['path'][:-1]
    if args.volume:
        archivepath = archivepath + '/' + args.volume

    logger.info('Starting Ingest for: %s', archivepath)

    # Possible bug in RQ?  Can't add to queue in "if fname == voldesc"
    voldescs = []
    for dirpath, dirs, files in os.walk(archivepath):
        for filename in files:
            fname = os.path.join(dirpath, filename)
            if args.search:
                if args.search in fname:
                    try:
                        if os.path.basename(fname) == "voldesc.cat":
                            voldescs.append(fname)
                        if args.ingest:
                            RQ_ingest.QueueAdd(fname)
                    except:
                        logger.error('File %s NOT added to Ingest Queue', fname)
                else:
                    continue
            else:
                try:
                    if os.path.basename(fname) == "voldesc.cat":
                        voldescs.append(fname)
                    if args.ingest:
                        RQ_ingest.QueueAdd(fname)
                except:
                    logger.error('File %s NOT added to Ingest Queue', fname)

    Qsize = RQ_ingest.QueueSize()
    for fpath in voldescs:
        RQ_linking.QueueAdd((fpath, args.archive))
    logger.info('Files added to Ingest Queue: %s', Qsize)

    logger.info('IngestJobber Complete')


if __name__ == "__main__":
    sys.exit(main())
