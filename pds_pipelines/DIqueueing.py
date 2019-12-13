#!/usr/bin/env python

import sys
import datetime
import logging
import argparse
import json
import pytz

from pds_pipelines.RedisQueue import RedisQueue
from pds_pipelines.db import db_connect
from pds_pipelines.models.pds_models import Files
from pds_pipelines.config import pds_info, pds_db, pds_log
from sqlalchemy import Date, cast
from sqlalchemy import or_


def parse_args():
    parser = argparse.ArgumentParser(description='PDS DI Data Integrity')

    parser.add_argument('--archive', '-a', dest="archive", required=True,
                        help="Enter archive - archive to ingest")

    parser.add_argument('--volume', '-v', dest="volume",
                        help="Enter volume to Ingest")

    parser.add_argument('--jobarray', '-j', dest="jobarray",
                        help="Enter string to set job array size")

    parser.add_argument('--log', '-l', dest="log_level",
                        choices=['DEBUG', 'INFO',
                                'WARNING', 'ERROR', 'CRITICAL'],
                        help="Set the log level.", default='INFO')

    args = parser.parse_args()
    return args


def main(user_args):
    archive = user_args.archive
    volume = user_args.volume
    jobarray = user_args.jobarray
    log_level = user_args.log_level

    RQ = RedisQueue('DI_ReadyQueue')

    PDSinfoDICT = json.load(open(pds_info, 'r'))
    try:
        archiveID = PDSinfoDICT[archive]['archiveid']
    except KeyError:
        print("\nArchive '{}' not found in {}\n".format(archive, pds_info))
        print("The following archives are available:")
        for k in PDSinfoDICT.keys():
            print("\t{}".format(k))
        exit()

    logger = logging.getLogger('DI_Queueing.' + archive)
    level = logging.getLevelName(log_level)
    logger.setLevel(level)
    logFileHandle = logging.FileHandler(pds_log + 'DI.log')
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s, %(message)s')
    logFileHandle.setFormatter(formatter)
    logger.addHandler(logFileHandle)

    logger.info("DI Queue: %s", RQ.id_name)

    logger.info('Starting %s DI Queueing', archive)
    if volume:
        logger.info('Queueing %s Volume', volume)

    try:
        session, _ = db_connect(pds_db)
        logger.info('DataBase Connecton: Success')
    except:
        logger.error('DataBase Connection: Error')
        return 1

    if volume:
        volstr = '%' + volume + '%'
        vol_exists = session.query(Files).filter(
            Files.archiveid == archiveID, Files.filename.like(volstr)).first()
        if not vol_exists:
            print(f"No files exist in the database for volume \"{volume}\"."
            "  Either the volume does not exist or it has not been properly ingested.\n")
            exit()

    td = (datetime.datetime.now(pytz.utc) -
          datetime.timedelta(days=30)).strftime("%Y-%m-%d %H:%M:%S")
    testing_date = datetime.datetime.strptime(str(td), "%Y-%m-%d %H:%M:%S")

    if volume:
        volstr = '%' + volume + '%'
        testQ = session.query(Files).filter(
            Files.archiveid == archiveID, Files.filename.like(volstr)).filter(
                or_(cast(Files.di_date, Date) < testing_date,
                    cast(Files.di_date, Date) is None))
    else:
        testQ = session.query(Files).filter(Files.archiveid == archiveID).filter(
            or_(cast(Files.di_date, Date) < testing_date,
                cast(Files.di_date, Date) is None))

    addcount = 0
    for element in testQ:
        try:
            RQ.QueueAdd((element.filename, archive))
            addcount = addcount + 1
        except:
            logger.warn('File %s Not Added to DI_ReadyQueue',
                         element.filename)

    logger.info('Files Added to Queue %s', addcount)
    logger.info('DI Queueing Complete')


if __name__ == "__main__":
    sys.exit(main(parse_args()))
