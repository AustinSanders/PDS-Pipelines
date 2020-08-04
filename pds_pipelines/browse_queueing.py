#!/usr/bin/env python

import sys
import logging
import argparse
import json
import pathlib
import glob
from os.path import getsize, dirname, splitext, exists, join, basename
from shutil import copy2, disk_usage
from pds_pipelines.redis_queue import RedisQueue
from pds_pipelines.db import db_connect
from pds_pipelines.models.pds_models import Files
from pds_pipelines.config import pds_info, pds_log, pds_db, workarea, disk_usage_ratio, archive_base

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
    error_queue = RedisQueue('UPC_ErrorQueue')
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

    if search:
        qf = '%' + search + '%'
        qOBJ = qOBJ.filter(Files.filename.like(qf))

    if qOBJ:
        path = PDSinfoDICT[archive]['path']
        addcount = 0
        size = 0
        for element in qOBJ:
            fname = path + element.filename
            size += getsize(fname)

        size_free = disk_usage(workarea).free
        if size >= (disk_usage_ratio * size_free):
            logger.error("Unable to process %s: size %d exceeds %d",
                         volume, size, (size_free * disk_usage_ratio))
            exit()

        for element in qOBJ:
            fname = path + element.filename
            fid = element.fileid

            try:
                dest_path = dirname(fname)
                dest_path = dest_path.replace(archive_base, workarea)
                pathlib.Path(dest_path).mkdir(parents=True, exist_ok=True)
                for f in glob.glob(splitext(fname)[0] + r'.*'):
                    if not exists(f'{dest_path}{f}'):
                        copy2(f, dest_path)

                RQ.QueueAdd((join(dest_path,basename(element.filename)), fid, archive))
                addcount = addcount + 1
            except Exception as e:
                error_queue.QueueAdd(f'Unable to copy / queue {fname}: {e}')
                logger.error('Unable to copy / queue %s: %s', fname, e)

        logger.info('Files Added to Browse Queue: %s', addcount)

if __name__ == "__main__":
    sys.exit(main(parse_args()))
