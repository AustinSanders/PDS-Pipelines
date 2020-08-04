#!/usr/bin/env python
import os
import sys
import pvl
import json
import datetime
import pytz
import logging
import errno
from pysis import isis
from pysis.exceptions import ProcessError
from sqlalchemy.orm.attributes import flag_modified

from pysis.isis import getsn
from ast import literal_eval

from pds_pipelines.redis_queue import RedisQueue
from pds_pipelines.redis_lock import RedisLock
from pds_pipelines.recipe import Recipe
from pds_pipelines.process import Process
from pds_pipelines.db import db_connect
from pds_pipelines.models.upc_models import DataFiles, SearchTerms, JsonKeywords
from pds_pipelines.models.pds_models import ProcessRuns
from pds_pipelines.config import pds_log, pds_info, workarea, pds_db, upc_db, lock_obj, upc_error_queue, recipe_base, archive_base, derived_base, derived_url
from pds_pipelines.utils import generate_processes, process

def getISISid(infile):
    serial_num = getsn(from_=infile)
    if isinstance(serial_num, bytes):
        serial_num = serial_num.decode()
    newisisSerial = serial_num.replace('\n', '')
    return newisisSerial

def makedir(inputfile):
    temppath = os.path.dirname(inputfile).lower()
    finalpath = temppath.replace(workarea, derived_base)

    if not os.path.exists(finalpath):
        try:
            os.makedirs(finalpath, exist_ok=True)
        except OSError as e:
            if e.errno != errno.EEXIST:
                raise

    return finalpath


def add_url(input_file, upc_id, proc, session_maker):
    session = session_maker()
    outputfile = input_file.replace(derived_base, derived_url)
    q_record = session.query(JsonKeywords).filter(JsonKeywords.upcid==upc_id)
    params = {}
    old_json = q_record.first().jsonkeywords
    old_json[proc] = outputfile
    params['jsonkeywords'] = old_json
    
    #record.jsonkeywords = json.loads(json.dumps(params, indent=4))
    q_record.update(params, False)
    # By default, SQLAlchemy does not track changes to json, so we have
    # to manually flag that the data were changed.
    #flag_modified(record, 'jsonkeywords')
    session.commit()
    session.close()


def AddProcessDB(session_maker, fid, outvalue):
    session = session_maker()
    date = datetime.datetime.now(pytz.utc).strftime("%Y-%m-%d %H:%M:%S")

    processDB = ProcessRuns(fileid=fid,
                            process_date=date,
                            process_typeid='5',
                            process_out=outvalue)

    try:
        session.merge(processDB)
        session.commit()
        session.close()
        return 'SUCCESS'
    except:
        session.close()
        return 'ERROR'


def main():
    # Set up logging
    logger = logging.getLogger('Derived_Process')
    logger.setLevel(logging.INFO)
    logFileHandle = logging.FileHandler(pds_log + 'Process.log')
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s, %(message)s')
    logFileHandle.setFormatter(formatter)
    logger.addHandler(logFileHandle)

    RQ_thumbnail= RedisQueue('Thumbnail_ReadyQueue')
    RQ_browse = RedisQueue('Browse_ReadyQueue')
    RQ_error = RedisQueue(upc_error_queue)
    RQ_lock = RedisLock(lock_obj)
    RQ_lock.add({RQ_thumbnail.id_name: '1'})
    RQ_lock.add({RQ_browse.id_name: '1'})

    PDSinfoDICT = json.load(open(pds_info, 'r'))

    pds_session_maker, pds_session = db_connect(pds_db)
    upc_session_maker, upc_session = db_connect(upc_db)

    # Only checks lock for browse.  Not sure of a better way to handle this.
    if (int(RQ_thumbnail.QueueSize()) > 0 or int(RQ_browse.QueueSize())) and RQ_lock.available(RQ_browse.id_name):
        if int(RQ_thumbnail.QueueSize()) > 0:
            proc = "thumbnail"
            item = literal_eval(RQ_thumbnail.QueueGet())
        else:
            proc = "browse"
            item = literal_eval(RQ_browse.QueueGet())

        inputfile = item[0]
        fid = item[1]
        archive = item[2]
        if os.path.isfile(inputfile):
            recipe_file = recipe_base + "/" + archive + '.json'
            with open(recipe_file) as fp:
                recipe = json.load(fp)['reduced']
                recipe_string = json.dumps(recipe['recipe'])

            logger.info('Starting Process: %s', inputfile)

            final_path = makedir(inputfile)
            derived_product = os.path.join(final_path, os.path.splitext(os.path.basename(inputfile))[0] + "." + proc + ".jpg")


            width = recipe[proc]['width']
            height = recipe[proc]['height']

            processes, infile, _, _, workarea_pwd = generate_processes(inputfile,
                                                                       recipe_string,
                                                                       logger,
                                                                       width=width,
                                                                       height=height,
                                                                       proc=proc,
                                                                       derived_product=derived_product,
                                                                       workarea=workarea)
            failing_command = process(processes, workarea, logger)
            # Ideally we could check for failing_command is None, but warnings count as errors
            if os.path.exists(derived_product):
                upc_session = upc_session_maker()
                isis_id = getISISid(infile)
                datafile = upc_session.query(DataFiles).filter(DataFiles.isisid.like(f"%{isis_id}%")).first()
                upc_id = datafile.upcid
                add_url(derived_product, upc_id, proc, upc_session_maker)
                upc_session.close()
                #os.remove(infile)
                logger.info(f'{proc} Process Success: %s', inputfile)
                AddProcessDB(pds_session_maker, fid, 't')
            else:
                logger.error('Error: %s', failing_command)

        else:
            RQ_error.QueueAdd(f'Unable to locate or access {inputfile} during {proc} processing')
            logger.error('File %s Not Found', inputfile)


if __name__ == "__main__":
    sys.exit(main())
