#!/usr/bin/env python

import os
import sys
import subprocess
import logging
import shutil
import argparse
import pvl
import json

from pds_pipelines.config import lock_obj, pds_log, default_namespace, upc_error_queue, workarea
from pds_pipelines.redis_queue import RedisQueue
from pds_pipelines.redis_lock import RedisLock
from pds_pipelines.redis_hash import RedisHash
from pds_pipelines.pds_logging import Loggy
from pds_pipelines.pds_process_logging import SubLoggy
from pds_pipelines.utils import generate_processes, process, generate_log_json


def parse_args():
    parser = argparse.ArgumentParser(description='Processing for Projection on the Web')
    parser.add_argument('--key',
                        '-k',
                        dest='key',
                        help='Target key')
    parser.add_argument('--namespace',
                        '-n',
                        dest='namespace',
                        help='Target key')

    args = parser.parse_args()
    return args


def main(user_args):
    key = user_args.key
    namespace = user_args.namespace

    if namespace is None:
        namespace = default_namespace

    work_dir = os.path.join(workarea, key)
    RQ_file = RedisQueue(key + '_FileQueue', namespace)
    RQ_work = RedisQueue(key + '_WorkQueue', namespace)
    RQ_zip = RedisQueue(key + '_ZIP', namespace)
    RQ_loggy = RedisQueue(key + '_loggy', namespace)
    RQ_final = RedisQueue('FinalQueue', namespace)
    RQ_recipe = RedisQueue(key + '_recipe', namespace)
    RQ_error = RedisQueue(upc_error_queue, namespace)
    RHash = RedisHash(key + '_info')
    RHerror = RedisHash(key + '_error')
    RQ_lock = RedisLock(lock_obj)
    RQ_lock.add({'POW':'1'})

    if int(RQ_file.QueueSize()) > 0 and RQ_lock.available('POW'):
        jobFile = RQ_file.Qfile2Qwork(RQ_file.getQueueName(),
                                      RQ_work.getQueueName())

        # Setup system logging
        basename = os.path.splitext(os.path.basename(jobFile))[0]
        logger = logging.getLogger(key + '.' + basename)
        logger.setLevel(logging.INFO)

        logFileHandle = logging.FileHandler(pds_log + '/Service.log')
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s, %(message)s')
        logFileHandle.setFormatter(formatter)
        logger.addHandler(logFileHandle)

        logger.info('Starting POW Processing')

        # set up loggy
        loggyOBJ = Loggy(basename)


        # File Naming
        if '+' in jobFile:
            bandSplit = jobFile.split('+')
            inputFile = bandSplit[0]
            out_bands = '+' + bandSplit[1]
        else:
            inputFile = jobFile
            out_bands = ''

        status = 'success'
        recipe_string = RQ_recipe.RecipeGet()
        no_extension_inputfile = os.path.join(work_dir, os.path.splitext(os.path.basename(jobFile))[0])
        processes = generate_processes(jobFile, recipe_string, logger, no_extension_inputfile=no_extension_inputfile, out_bands=out_bands)
        failing_command, error = process(processes, work_dir, logger)
        process_log = generate_log_json(processes, basename, failing_command, error)

        if failing_command:
            status = 'error'

        if status == 'success':

            final_process, args = list(processes.items())[-1]
            if final_process == 'gdal_translate':
                finalfile = args['dest']
            else:
                finalfile = args['to']

            if RHash.getStatus() != 'ERROR':
                RHash.Status('SUCCESS')

            try:
                RQ_zip.QueueAdd(finalfile)
                logger.info('File Added to ZIP Queue')
            except:
                logger.error('File NOT Added to ZIP Queue')

        elif status == 'error':
            RHash.Status('ERROR')

        try:
            RQ_loggy.QueueAdd(process_log)
            RQ_work.QueueRemove(jobFile)
            logger.info('JSON Added to Loggy Queue')
        except:
            logger.error('JSON NOT Added to Loggy Queue')

        if RQ_file.QueueSize() == 0 and RQ_work.QueueSize() == 0:
            try:
                RQ_final.QueueAdd(key)
                logger.info('Key %s Added to Final Queue: Success', key)
                logger.info('Both Queues Empty: filequeue = %s  work queue = %s', str(
                    RQ_file.QueueSize()), str(RQ_work.QueueSize()))
                logger.info('JOB Complete')
            except:
                logger.error('Key NOT Added to Final Queue')
        elif RQ_file.QueueSize() == 0 and RQ_work.QueueSize() != 0:
            logger.warning('Work Queue Not Empty: filequeue = %s  work queue = %s', str(
                RQ_file.QueueSize()), str(RQ_work.QueueSize()))


if __name__ == "__main__":
    sys.exit(main(parse_args()))
