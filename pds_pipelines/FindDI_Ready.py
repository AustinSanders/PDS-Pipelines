#!/usr/bin/env python

import sys
import datetime
import time
import argparse
import json
import logging
import pytz

from sqlalchemy import Date, cast

from sqlalchemy import or_

from pds_pipelines.db import db_connect
from pds_pipelines.models.pds_models import Files
from pds_pipelines.config import pds_info, pds_db, pds_log


def parse_args():
    parser = argparse.ArgumentParser(description='Find Arcives for DI')

    parser.add_argument('--archive', '-a', dest="archive",
                        help="Enter archive to test for DI")

    parser.add_argument('--volume', '-v', dest="volume",
                        help="Enter Volume to Test")

    parser.add_argument('--log', '-l', dest="log_level",
                        choices=['DEBUG', 'INFO',
                                'WARNING', 'ERROR', 'CRITICAL'],
                        help="Set the log level.", default='INFO')


    args = parser.parse_args()
    return args


def archive_expired(session, archiveID, testing_date=None):
    """
    Checks to see if archive is expired

    Parameters
    ----------
    session
    archiveID
    testing_date

    Returns
    -------
    expired
    """
    if testing_date is None:
        td = (datetime.datetime.now(pytz.utc)
              - datetime.timedelta(days=30)).strftime("%Y-%m-%d %H:%M:%S")

        testing_date = datetime.datetime.strptime(str(td), "%Y-%m-%d %H:%M:%S")

    expired = session.query(Files).filter(
        Files.archiveid == archiveID).filter(or_(
            cast(Files.di_date, Date) < testing_date,
            cast(Files.di_date, Date) is None))

    return expired


def volume_expired(session, archiveID, volume, testing_date=None):
    """
    Checks to see if the volume is expired

    Parameters
    ----------
    session
    archiveID
    testing_date

    Returns
    -------
    expired
    """
    if testing_date is None:
        td = (datetime.datetime.now(pytz.utc)
              - datetime.timedelta(days=30)).strftime("%Y-%m-%d %H:%M:%S")

        testing_date = datetime.datetime.strptime(str(td), "%Y-%m-%d %H:%M:%S")

    volstr = '%' + volume + '%'
    expired = session.query(Files).filter(Files.archiveid == archiveID,
                                          Files.filename.like(volstr)).filter(or_(
                                              cast(Files.di_date,
                                                   Date) < testing_date,
                                              cast(Files.di_date, Date) is None))

    return expired


def main(archive, volume, log_level):
    PDSinfoDICT = json.load(open(pds_info, 'r'))
    try:
        archiveID = PDSinfoDICT[archive]['archiveid']
    except KeyError:
        print("\nArchive '{}' not found in {}\n".format(archive, pds_info))
        print("The following archives are available:")
        for k in PDSinfoDICT.keys():
            print("\t{}".format(k))
        exit()

    logger = logging.getLogger('DI_Ready.' + archive)
    level = logging.getLevelName(log_level)
    logger.setLevel(level)
    timestr = time.strftime("%Y%m%d_%H%M%S")
    logFileHandle = logging.FileHandler(pds_log + 'DI_Ready_' + timestr + '.log')
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s, %(message)s')
    logFileHandle.setFormatter(formatter)

    # Add handler to print to stdout
    logStreamHandle = logging.StreamHandler()
    logStreamHandle.setLevel(level)

    logger.addHandler(logFileHandle)
    logger.addHandler(logStreamHandle)

    try:
        # Throws away 'engine' information
        session, _ = db_connect(pds_db)
        logger.info("%s", archive)
        logger.info("Database Connection Success")
    except Exception as e:
        logger.error("%s", e)
    else:
        if volume:
            volstr = '%' + volume + '%'
            vol_exists = session.query(Files).filter(
                Files.archiveid == archiveID, Files.filename.like(volstr)).first()
            if not vol_exists:
                print(f"No files exist in the database for volume \"{volume}\"."
                "  Either the volume does not exist or it has not been properly ingested.\n")
                exit()
        # if db connection fails, there's no sense in doing this part
        td = (datetime.datetime.now(pytz.utc)
              - datetime.timedelta(days=30)).strftime("%Y-%m-%d %H:%M:%S")
        testing_date = datetime.datetime.strptime(str(td), "%Y-%m-%d %H:%M:%S")

        if volume:
            expired = volume_expired(session, archiveID, volume, testing_date)
            if expired.count():
                logger.info("Volume %s DI Ready: %s Files", volume,
                            str(expired.count()))
            else:
                logger.info('Volume %s DI Current', volume)
        else:
            expired = archive_expired(session, archiveID, testing_date)
            if expired.count():
                logger.info('Archive %s DI Ready: %s Files', archive,
                            str(expired.count()))
            else:
                logger.info('Archive %s DI Current', archive)


if __name__ == "__main__":
    args = parse_args()
    sys.exit(main(**vars(args)))
