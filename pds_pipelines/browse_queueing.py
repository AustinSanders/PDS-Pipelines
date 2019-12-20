#!/usr/bin/env python

import sys
import logging
import argparse
import json

from pds_pipelines.RedisQueue import RedisQueue
from pds_pipelines.db import db_connect
from pds_pipelines.models.pds_models import Files
from pds_pipelines.config import pds_info, pds_log, pds_db

def parse_args():

    parser = argparse.ArgumentParser(description='PDS DI Data Integrity')

    parser.add_argument('--archive', '-a', dest="archive", required=True,
                        help="Enter archive - archive to ingest")

    parser.add_argument('--volume', '-v', dest="volume",
                        help="Enter voluem to Ingest")

    parser.add_argument('--search', '-s', dest="search",
                        help="Enter string to search for")

    parser.add_argument('--log', '-l', dest="log_level",
                        choices=['DEBUG', 'INFO', 'WARNING',
                                'ERROR', 'CRITICAL'],
                        help="Set the log level.", default='INFO')

    args = parser.parse_args()
    return args

def main(user_args):
    archive = user_args.archive
    volume = user_args.volume
    search = user_args.search
    log_level = user_args.log_level

    logger = logging.getLogger('Browse_Queueing.' + archive)
    level = logging.getLevelName(log_level)
    logger.setLevel(level)
    logFileHandle = logging.FileHandler(pds_log+'Process.log')
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s, %(message)s')
    logFileHandle.setFormatter(formatter)
    logger.addHandler(logFileHandle)

    logger.info('Starting Process')

    PDSinfoDICT = json.load(open(pds_info, 'r'))
    archiveID = PDSinfoDICT[archive]['archiveid']

    RQ = RedisQueue('Browse_ReadyQueue')
    logger.info("Browse Queue: %s", RQ.id_name)

    try:
        Session, _ = db_connect(pds_db)
        session = Session()
        logger.info('Database Connection Success')
    except:
        logger.error('Database Connection Error')
        return 1

    if volume:
        volstr = '%' + volume + '%'
        qOBJ = session.query(Files).filter(Files.archiveid == archiveID,
                                           Files.filename.like(volstr),
                                           Files.upc_required == 't')
    else:
        qOBJ = session.query(Files).filter(Files.archiveid == archiveID,
                                           Files.upc_required == 't')
    if qOBJ:
        addcount = 0
        for element in qOBJ:
            fname = PDSinfoDICT[archive]['path'] + element.filename
            fid = element.fileid
            RQ.QueueAdd((fname, fid, archive))
            addcount = addcount + 1

        logger.info('Files Added to UPC Queue: %s', addcount)


if __name__ == "__main__":
    sys.exit(main(parse_args()))
