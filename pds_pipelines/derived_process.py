#!/usr/bin/env python
import os
import sys
import json
import logging
import errno

from ast import literal_eval
from json import JSONDecoder

from pds_pipelines.redis_queue import RedisQueue
from pds_pipelines.redis_lock import RedisLock
from pds_pipelines.db import db_connect
from pds_pipelines.models.upc_models import DataFiles, SearchTerms, JsonKeywords
from pds_pipelines.config import pds_log, pds_info, workarea, pds_db, upc_db, lock_obj, upc_error_queue, recipe_base, archive_base, derived_base, derived_url
from pds_pipelines.utils import generate_processes, process, parse_pairs, add_process_db, get_isis_id

def makedir(inputfile):
    temppath = os.path.dirname(inputfile)
    finalpath = temppath.replace(workarea, derived_base)

    if not os.path.exists(finalpath):
        try:
            os.makedirs(finalpath, exist_ok=True)
        except OSError as e:
            if e.errno != errno.EEXIST:
                raise

    return finalpath


def add_url(input_file, upc_id, session_maker):
    session = session_maker()
    outputfile = input_file.replace(derived_base, derived_url)
    thumb = outputfile + '.thumbnail.jpg'
    browse = outputfile + '.browse.jpg'
    q_record = session.query(JsonKeywords).filter(JsonKeywords.upcid==upc_id)
    params = {}
    old_json = q_record.first().jsonkeywords
    old_json['browse'] = browse
    old_json['thumbnail'] = thumb
    params['jsonkeywords'] = old_json

    q_record.update(params, False)
    session.commit()
    session.close()


def main():
    # Set up logging
    logger = logging.getLogger('Derived_Process')
    logger.setLevel(logging.INFO)
    logFileHandle = logging.FileHandler(pds_log + 'Process.log')
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s, %(message)s')
    logFileHandle.setFormatter(formatter)
    logger.addHandler(logFileHandle)

    RQ_derived = RedisQueue('Derived_ReadyQueue')
    RQ_error = RedisQueue(upc_error_queue)
    RQ_lock = RedisLock(lock_obj)
    RQ_lock.add({RQ_derived.id_name: '1'})

    PDSinfoDICT = json.load(open(pds_info, 'r'))

    pds_session_maker, pds_session = db_connect(pds_db)
    upc_session_maker, upc_session = db_connect(upc_db)

    # Only checks lock for browse.  Not sure of a better way to handle this.
    if int(RQ_derived.QueueSize()) and RQ_lock.available(RQ_derived.id_name):
        item = literal_eval(RQ_derived.QueueGet())

        inputfile = item[0]
        fid = item[1]
        archive = item[2]
        if os.path.isfile(inputfile):
            recipe_file = recipe_base + "/" + archive + '.json'
            with open(recipe_file) as fp:
                recipe = json.load(fp, object_pairs_hook = parse_pairs)['reduced']
                recipe_string = json.dumps(recipe['recipe'])

            logger.info('Starting Process: %s', inputfile)

            final_path = makedir(inputfile)
            derived_product = os.path.join(final_path, os.path.splitext(os.path.basename(inputfile))[0])

            no_extension_inputfile = os.path.splitext(inputfile)[0]
            processes = generate_processes(inputfile,
                                           recipe_string,
                                           logger,
                                           no_extension_inputfile=no_extension_inputfile, 
                                           derived_product=derived_product)
            failing_command = process(processes, workarea, logger)
            if not failing_command:
                upc_session = upc_session_maker()
                isis_id = get_isis_id(no_extension_inputfile)
                datafile = upc_session.query(DataFiles).filter(DataFiles.isisid.like(f"%{isis_id}%")).first()
                upc_id = datafile.upcid
                add_url(derived_product, upc_id, upc_session_maker)
                upc_session.close()
                #os.remove(infile)
                logger.info(f'Derived Process Success: %s', inputfile)
                add_process_db(pds_session_maker, fid, 't')
            else:
                logger.error('Error: %s', failing_command)

        else:
            RQ_error.QueueAdd(f'Unable to locate or access {inputfile} during derived processing')
            logger.error('File %s Not Found', inputfile)


if __name__ == "__main__":
    sys.exit(main())
