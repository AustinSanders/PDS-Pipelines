#!/usr/bin/env python

import os
import sys
import json
import subprocess
import logging
import shutil
import argparse

from pds_pipelines.config import lock_obj, pds_log, upc_error_queue, default_namespace, workarea
from pds_pipelines.redis_queue import RedisQueue
from pds_pipelines.redis_lock import RedisLock
from pds_pipelines.redis_hash import RedisHash
from pds_pipelines.pds_logging import Loggy
from pds_pipelines.pds_process_logging import SubLoggy
from pds_pipelines.process import Process
from pds_pipelines.utils import generate_processes, process, generate_log_json


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--key',
                        '-k',
                        dest='key',
                        help="Target key -- if blank, process first element in queue")
    parser.add_argument('--namespace',
                        '-n',
                        dest='namespace',
                        help="Queue namespace")
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
    RQ_lock.add({'MAP':'1'})

    if int(RQ_file.QueueSize()) > 0 and RQ_lock.available('MAP'):
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

        logger.info('Starting MAP Processing')

        # File Naming
        infile = os.path.join(work_dir, \
            os.path.splitext(os.path.basename(jobFile))[0] + '.input.cub')
        outfile = os.path.join(work_dir,
            os.path.splitext(os.path.basename(jobFile))[0] + '.output.cub')

        # Recipe Stuff
        status = 'success'
        recipe_string = RQ_recipe.QueueGet()
        no_extension_inputfile = os.path.join(work_dir, os.path.splitext(os.path.basename(jobFile))[0])
        processes = generate_processes(jobFile, recipe_string, logger, no_extension_inputfile=no_extension_inputfile)
        failing_command, error = process(processes, work_dir, logger)
        process_log = generate_log_json(processes, basename, failing_command, error)

        if failing_command:
            status = 'error'

        if status == 'success':
            final_file_list = []
            img_format = RHash.Format()

            # If final output format is ISIS3 or PDS, will use an ISIS program to create
            if img_format == 'ISIS3' or img_format == 'PDS':
                last_output = list(processes.items())[-1][-1]['to']
                last_output = last_output.split('+')[0]
                if img_format == 'ISIS3':
                    finalfile = os.path.join(work_dir, RHash.getMAPname() + '.cub')
                else:
                    finalfile = os.path.join(work_dir, RHash.getMAPname() + '.img')

            # Else will use GDAL, so set extension and define possible ancillary files
            else:
                if img_format == 'GeoTiff-BigTiff' or img_format == 'GTiff':
                    fileext = 'tif'
                elif img_format == 'GeoJPEG-2000':
                    fileext = 'jp2'
                elif img_format == 'JPEG':
                    fileext = 'jpg'
                elif img_format == 'PNG':
                    fileext = 'png'
                elif img_format == 'GIF':
                    fileext = 'gif'

                last_output = list(processes.items())[-1][-1]['dest']
                finalfile = os.path.join(work_dir, RHash.getMAPname() + '.' + fileext)

                # Possible ancillary files
                last_output_msk = last_output + '.msk'
                last_output_aux = last_output + '.aux.xml'

                if os.path.isfile(last_output_msk):
                    finalfile_msk = os.path.join(work_dir, RHash.getMAPname() + '.' + fileext + '.msk')
                    shutil.move(last_output_msk, finalfile_msk)
                    final_file_list.append(finalfile_msk)

                if os.path.isfile(last_output_aux):
                    finalfile_aux = os.path.join(work_dir, RHash.getMAPname() + '.' + fileext + '.aux.xml')
                    shutil.move(last_output_aux, finalfile_aux)
                    final_file_list.append(finalfile_aux)

            shutil.move(last_output, finalfile)
            final_file_list.append(finalfile)


            if RHash.getStatus() != 'ERROR':
                RHash.Status('SUCCESS')

            # Loop over the list of final output files and add them to RQ_zip
            for item in final_file_list:
                try:
                    RQ_zip.QueueAdd(item)
                    logger.info('File Added to ZIP Queue')
                except:
                    logger.error('File NOT Added to ZIP Queue')

            try:
                RQ_loggy.QueueAdd(process_log)
                logger.info('JSON Added to Loggy Queue')
            except Exception as e:
                logger.error(f'JSON NOT Added to Loggy Queue with error: {e}')

            RQ_work.QueueRemove(jobFile)

        elif status == 'error':
            RHash.Status('ERROR')
            logger.error(f'Process {failing_command} :: Error')
            logger.error(error)
            error_string = f'Error Executing {failing_command}' \
                           f'Standard Error: {error}'
            RHerror.addError(basename, error_string)
            RQ_error.QueueAdd(f'Process {failing_command} failed for {jobFile}')
            if os.path.isfile(infile):
                os.remove(infile)

        if RQ_file.QueueSize() == 0 and RQ_work.QueueSize() == 0:
            try:
                RQ_final.QueueAdd(key)
                logger.info('Key %s Added to Final Queue: Success', key)
                logger.info('Job Complete')
            except:
                logger.error('Key NOT Added to Final Queue')
        else:
            logger.warning('Queues Not Empty: filequeue = %s  work queue = %s', str(
                RQ_file.QueueSize()), str(RQ_work.QueueSize()))


if __name__ == "__main__":
    sys.exit(main(parse_args()))
