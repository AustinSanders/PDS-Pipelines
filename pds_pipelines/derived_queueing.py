#!/usr/bin/env python
import sys
import logging
import argparse
import json
import pathlib
import glob
from shutil import copy2, disk_usage
from os.path import getsize, dirname, splitext, exists, basename, join
from pds_pipelines.redis_queue import RedisQueue
from pds_pipelines.db import db_connect
from pds_pipelines.models.pds_models import Files
from pds_pipelines.config import pds_info, pds_log, pds_db, workarea, disk_usage_ratio, archive_base

class Args:
    def __init__(self):
        pass

    def parse_args(self):

        parser = argparse.ArgumentParser(description='PDS DI Data Integrity')

        parser.add_argument('--archive', '-a', dest="archive", required=True,
                          help="Enter archive - archive to ingest")

        parser.add_argument('--volume', '-v', dest="volume",
                          help="Enter voluem to Ingest")

        parser.add_argument('--search', '-s', dest="search",
                          help="Enter string to search for")

        args = parser.parse_args()

        self.archive = args.archive
        self.volume = args.volume
        self.search = args.search


def main():

#    pdb.set_trace()
    args = Args()
    args.parse_args()

    logger = logging.getLogger('Derived_Queueing.' + args.archive)
    logger.setLevel(logging.INFO)
    logFileHandle = logging.FileHandler(pds_log + 'Process.log')
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s, %(message)s')
    logFileHandle.setFormatter(formatter)
    logger.addHandler(logFileHandle)

    logger.info('Starting Process')

    PDSinfoDICT = json.load(open(pds_info, 'r'))
    archiveID = PDSinfoDICT[args.archive]['archiveid']

    RQ = RedisQueue('Derived_ReadyQueue')
    error_queue = RedisQueue('UPC_ErrorQueue')

    try:
        Session, _ = db_connect(pds_db)
        session = Session()
        logger.info('Database Connection Success')
    except:
        logger.error('Database Connection Error')

    if args.volume:
        volstr = '%' + args.volume + '%'
        qOBJ = session.query(Files).filter(Files.archiveid == archiveID,
                                                Files.filename.like(volstr),
                                                Files.upc_required == 't')
    else:
        qOBJ = session.query(Files).filter(Files.archiveid == archiveID,
                                             Files.upc_required == 't')


    if args.search:
        qf = '%' + args.search + '%'
        qOBJ = qOBJ.filter(Files.filename.like(qf))

    if qOBJ:
        path = PDSinfoDICT[args.archive]['path']
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

                RQ.QueueAdd((join(dest_path,basename(element.filename)), fid, args.archive))
                addcount = addcount + 1
            except Exception as e:
                error_queue.QueueAdd(f'Unable to copy / queue {fname}: {e}')
                logger.error('Unable to copy / queue %s: %s', fname, e)

        logger.info('Files Added to Derived Queue: %s', addcount)

if __name__ == "__main__":
    sys.exit(main())
