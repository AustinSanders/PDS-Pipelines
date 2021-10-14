#!/usr/bin/env python
import os
import sys
import logging
from ast import literal_eval
import argparse
import json
from pds_pipelines.available_modules import *
from pds_pipelines.redis_lock import RedisLock
from pds_pipelines.redis_queue import RedisQueue
from pds_pipelines.config import pds_log, workarea, lock_obj, upc_error_queue, recipe_base
from pds_pipelines.utils import generate_processes, process,  parse_pairs


def parse_args():
    parser = argparse.ArgumentParser(description='UPC Processing')
    parser.add_argument('--no-upc', dest='proc', help='Do not perform UPC processing',
                        action="store_false")
    parser.add_argument('--no-derived', dest='derived', help='Do not perform derived processing',
                        action="store_false")
    parser.add_argument('--log', '-l', dest="log_level",
                        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'],
                        help="Set the log level.", default='INFO')
    parser.set_defaults(proc=True)
    parser.set_defaults(derived=True)
    parser.add_argument('--namespace', '-n', dest="namespace",
                        help="The namespace used for this queue.")
    args = parser.parse_args()
    return args


def main(user_args):
    proc = user_args.proc
    derived = user_args.derived
    log_level = user_args.log_level
    namespace = user_args.namespace

    try:
        slurm_job_id = os.environ['SLURM_ARRAY_JOB_ID']
        slurm_array_id = os.environ['SLURM_ARRAY_TASK_ID']
    except:
        slurm_job_id = ''
        slurm_array_id = ''

    inputfile = ''
    context = {'job_id': slurm_job_id, 'array_id':slurm_array_id, 'inputfile': inputfile}
    logger = logging.getLogger('UPC_Process')
    level = logging.getLevelName(log_level)
    logger.setLevel(level)
    log_file_handle = logging.FileHandler(pds_log + 'Process.log')
    formatter = logging.Formatter(
        '%(asctime)s - %(job_id)s - %(array_id)s - %(inputfile)s - %(name)s - %(levelname)s, %(message)s')
    log_file_handle.setFormatter(formatter)
    logger.addHandler(log_file_handle)
    logger = logging.LoggerAdapter(logger, context)

    # Redis Queue Objects
    RQ_main = RedisQueue('UPC_ReadyQueue', namespace)
    RQ_error = RedisQueue(upc_error_queue, namespace)
    RQ_work = RedisQueue('UPC_WorkQueue', namespace)
    RQ_update = RedisQueue('UPC_UpdateQueue', namespace)

    logger.info("UPC Processing Queue: %s", RQ_main.id_name)

    RQ_error = RedisQueue(upc_error_queue)
    RQ_lock = RedisLock(lock_obj)
    # If the queue isn't registered, add it and set it to "running"
    RQ_lock.add({RQ_main.id_name: '1'})


    # if there are items in the redis queue
    if int(RQ_main.QueueSize()) > 0 and RQ_lock.available(RQ_main.id_name):
        # get a file from the queue
        item = RQ_main.Qfile2Qwork(RQ_main.getQueueName(), RQ_work.getQueueName())
        inputfile = literal_eval(item)[0]
        archive = literal_eval(item)[1]

        if not os.path.isfile(inputfile):
            RQ_error.QueueAdd(f'Unable to locate or access {inputfile} during UPC processing')
            logger.debug("%s is not a file\n", inputfile)
            exit()

        # Update the logger context to include inputfile
        context['inputfile'] = inputfile

        recipe_file = recipe_base + "/" + archive + '.json'

        no_extension_inputfile = os.path.splitext(inputfile)[0]
        cam_info_file = no_extension_inputfile + '_caminfo.pvl'
        footprint_file = no_extension_inputfile + '_footprint.json'
        catlab_output = no_extension_inputfile + '_catlab.pvl'

        if proc:
            with open(recipe_file) as fp:
                upc_json = json.load(fp)['upc']
                recipe_string = json.dumps(upc_json['recipe'])

            processes = generate_processes(inputfile,
                                           recipe_string, logger,
                                           no_extension_inputfile=no_extension_inputfile,
                                           catlab_output=catlab_output,
                                           cam_info_file=cam_info_file,
                                           footprint_file=footprint_file)
            failing_command, _ = process(processes, workarea, logger)
            RQ_update.QueueAdd((inputfile, archive, failing_command, 'upc'))

        if derived:
            if os.path.isfile(inputfile):
                recipe_file = recipe_base + "/" + archive + '.json'
                with open(recipe_file) as fp:
                    recipe = json.load(fp, object_pairs_hook=parse_pairs)['reduced']
                    recipe_string = json.dumps(recipe['recipe'])

                logger.info('Starting Process: %s', inputfile)

                work_dir = os.path.dirname(inputfile)
                derived_product = os.path.join(work_dir, os.path.splitext(os.path.basename(inputfile))[0])

                no_extension_inputfile = os.path.splitext(inputfile)[0]
                processes = generate_processes(inputfile,
                                               recipe_string,
                                               logger,
                                               no_extension_inputfile=no_extension_inputfile,
                                               derived_product=derived_product)
                failing_command, _ = process(processes, workarea, logger)

                if not failing_command:
                    RQ_update.QueueAdd((inputfile, archive, failing_command, 'derived'))
                else:
                    logger.error('Error: %s', failing_command)

        RQ_work.QueueRemove(item)
    logger.info("UPC processing exited")

if __name__ == "__main__":
    sys.exit(main(parse_args()))
