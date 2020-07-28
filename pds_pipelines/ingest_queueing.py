#!/usr/bin/env python

import os
import sys
import json
import logging
import argparse

from pds_pipelines.redis_queue import RedisQueue
from pds_pipelines.config import pds_info, pds_log


def parse_args():
    parser = argparse.ArgumentParser(description='PDS DI Database Ingest')

    parser.add_argument('--archive', '-a', dest="archive", required=True,
                        help="Enter archive - archive to ingest")

    parser.add_argument('--volume', '-v', dest="volume",
                        help="Enter volume to Ingest")

    parser.add_argument('--log', '-l', dest="log_level",
                        choices=['DEBUG', 'INFO', 'WARNING',
                                'ERROR', 'CRITICAL'],
                        help="Set the log level.", default='INFO')

    parser.add_argument('--search', '-s', dest="search",
                        help="Enter string to search for")

    parser.add_argument('--link-only', dest='ingest', action='store_false')
    parser.set_defaults(ingest=True)
    parser.set_defaults(search='')

    args = parser.parse_args()
    return args


def main(user_args):
    archive = user_args.archive
    volume = user_args.volume
    search = user_args.search
    log_level = user_args.log_level
    ingest = user_args.ingest

    RQ_ingest = RedisQueue('Ingest_ReadyQueue')
    RQ_linking = RedisQueue('LinkQueue')

    # Set up logging
    logger = logging.getLogger(archive + '_INGEST')
    level = logging.getLevelName(log_level)
    logger.setLevel(level)
    logFileHandle = logging.FileHandler(pds_log + 'Ingest.log')
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s, %(message)s')
    logFileHandle.setFormatter(formatter)
    logger.addHandler(logFileHandle)

    print("Log File: {}Ingest.log".format(pds_log))

    PDSinfoDICT = json.load(open(pds_info, 'r'))
    try:
        archivepath = PDSinfoDICT[archive]['path'][:-1]
    except KeyError:
        print("\nArchive '{}' not found in {}\n".format(archive, pds_info))
        print("The following archives are available:")
        for k in PDSinfoDICT.keys():
            print("\t{}".format(k))
        logging.error("Unable to locate %s", archive)
        exit()

    if volume:
        archivepath = archivepath + '/' + volume

    logger.info('Starting Ingest for: %s', archivepath)
    logger.info('Ingest Queue: %s', str(RQ_ingest.id_name))
    logger.info('Linking Queue: %s', str(RQ_linking.id_name))

    # Possible bug in RQ?  Can't add to queue in "if fname == voldesc"
    queue_size = RQ_ingest.QueueSize()
    voldescs = []
    for dirpath, _, files in os.walk(archivepath):
        for filename in files:
            fname = os.path.join(dirpath, filename)
            if search in fname:
                try:
                    if os.path.basename(fname).lower() == "voldesc.cat":
                        voldescs.append(fname)
                    if ingest:
                        RQ_ingest.QueueAdd((fname, archive))
                except Exception as e:
                    logger.warn('File %s NOT added to Ingest Queue: %s', fname, str(e))
            else:
                continue

    n_added = RQ_ingest.QueueSize() - queue_size
    for fpath in voldescs:
        RQ_linking.QueueAdd((fpath, archive))
    logger.info('Files added to Ingest Queue: %s', n_added)


if __name__ == "__main__":
    sys.exit(main(parse_args()))
