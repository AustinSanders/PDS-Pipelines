#!/usr/bin/env python
import os
import sys
import datetime
import logging
import json
import argparse
import hashlib
import pytz

from ast import literal_eval
from pds_pipelines.redis_queue import RedisQueue
from pds_pipelines.redis_lock import RedisLock
from pds_pipelines.db import db_connect
from pds_pipelines.config import pds_info, pds_log, pds_db, archive_base, web_base, lock_obj, upc_error_queue
from pds_pipelines.models.pds_models import Files


def parse_args():

    parser = argparse.ArgumentParser(description='PDS DI Database Ingest')

    parser.add_argument('--override', dest='override', action='store_true')
    parser.set_defaults(override=False)

    parser.add_argument('--log', '-l', dest="log_level",
                        choices=['DEBUG', 'INFO',
                                'WARNING', 'ERROR', 'CRITICAL'],
                        help="Set the log level.", default='INFO')

    args = parser.parse_args()
    return args


def main(user_args):
    log_level = user_args.log_level
    override = user_args.override

    logger = logging.getLogger('Ingest_Process')
    level = logging.getLevelName(log_level)
    logger.setLevel(level)
    logFileHandle = logging.FileHandler(pds_log + 'Ingest.log')
    print("Log File: {}Ingest.log".format(pds_log))
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s, %(message)s')
    logFileHandle.setFormatter(formatter)
    logger.addHandler(logFileHandle)

    logger.info("Starting Ingest Process")
    PDSinfoDICT = json.load(open(pds_info, 'r'))

    RQ_main = RedisQueue('Ingest_ReadyQueue')
    RQ_error = RedisQueue(upc_error_queue)
    RQ_lock = RedisLock(lock_obj)
    RQ_lock.add({RQ_main.id_name: '1'})
    RQ_work = RedisQueue('Ingest_WorkQueue')

    try:
        Session, engine = db_connect(pds_db)
        session = Session()
        logger.info('DataBase Connecton: Success')
    except:
        logger.error('DataBase Connection: Error')
        return 1

    index = 1

    while int(RQ_main.QueueSize()) > 0 and RQ_lock.available(RQ_main.id_name):
        item = RQ_main.Qfile2Qwork(RQ_main.getQueueName(), RQ_work.getQueueName())
        inputfile = literal_eval(item)[0]
        archive = literal_eval(item)[1]

        if not os.path.isfile(inputfile):
            RQ_error.QueueAdd(f'Unable to locate or access {inputfile} during ingest processing')
            logger.warning("%s is not a file\n", inputfile)
            continue

        subfile = inputfile.replace(PDSinfoDICT[archive]['path'], '')
        # Calculate checksum in chunks of 4096
        f_hash = hashlib.md5()
        with open(inputfile, "rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                f_hash.update(chunk)
        filechecksum = f_hash.hexdigest()

        QOBJ = session.query(Files).filter_by(filename=subfile).first()

        runflag = False
        if QOBJ is None or filechecksum != QOBJ.checksum:
            runflag = True

        if runflag or override:
            date = datetime.datetime.now(
                pytz.utc).strftime("%Y-%m-%d %H:%M:%S")
            fileURL = inputfile.replace(archive_base, web_base)

            # If all upc requirements are in 'inputfile,' flag for upc
            try:
                upcflag = all(req in inputfile for req in PDSinfoDICT[archive]['upc_reqs'])
            except KeyError:
                logger.debug("No upc_reqs found for %s\nSetting upc eligibility False for all related files.", str(archive))
                upcflag = False

            filesize = os.path.getsize(inputfile)

            try:
                ingest_entry = Files()

                if QOBJ is not None and override:
                    ingest_entry.fileid = QOBJ.fileid

                ingest_entry.archiveid = PDSinfoDICT[archive]['archiveid']
                ingest_entry.filename = subfile
                ingest_entry.entry_date = date
                ingest_entry.checksum = filechecksum
                ingest_entry.upc_required = upcflag
                ingest_entry.validation_required = True
                ingest_entry.header_only = False
                ingest_entry.release_date = date
                ingest_entry.file_url = fileURL
                ingest_entry.file_size = filesize
                ingest_entry.di_pass = True
                ingest_entry.di_date = date

                session.merge(ingest_entry)
                session.flush()

                RQ_work.QueueRemove(item)

                index = index + 1

            except Exception as e:
                logger.error("Error During File Insert %s : %s", str(subfile), str(e))

        elif not runflag and not override:
            RQ_work.QueueRemove(item)
            logger.warning("Not running ingest: file %s already present"
                        " in database and no override flag supplied", inputfile)

        if index >= 250:
            try:
                session.commit()
                logger.info("Commit 250 files to Database: Success")
                index = 1
            except Exception as e:
                session.rollback()
                logger.warning("Unable to commit to database: %s", str(e))
    else:
        logger.info("No Files Found in Ingest Queue")
        try:
            session.commit()
            logger.info("Commit to Database: Success")
        except Exception as e:
            logger.error("Unable to commit to database: %s", str(e))
            session.rollback()


    # Close connection to database
    session.close()
    engine.dispose()

    if RQ_main.QueueSize() == 0 and RQ_work.QueueSize() == 0:
        logger.info("Process Complete All Queues Empty")
    elif RQ_main.QueueSize() == 0 and RQ_work.QueueSize() != 0:
        logger.warning("Process Done Work Queue NOT Empty Contains %s Files", str(
            RQ_work.QueueSize()))

    logger.info("Ingest Complete")


if __name__ == "__main__":
    sys.exit(main(parse_args()))
