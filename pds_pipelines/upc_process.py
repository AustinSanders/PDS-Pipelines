#!/usr/bin/env python
import re
import os
import sys
import datetime
import logging
import hashlib
import pds_pipelines
from ast import literal_eval
import pytz
import argparse
import jinja2
from glob import glob

import pvl
import json
from sqlalchemy import and_
from pds_pipelines.available_modules import *
from osgeo import ogr

from pysis import isis
from pysis.exceptions import ProcessError

from pds_pipelines.redis_lock import RedisLock
from pds_pipelines.redis_queue import RedisQueue
from pds_pipelines.recipe import Recipe
from pds_pipelines.process import Process
from pds_pipelines.upc_keywords import UPCkeywords
from pds_pipelines.db import db_connect
from pds_pipelines.models import pds_models
from pds_pipelines.models.upc_models import SearchTerms, Targets, Instruments, DataFiles, JsonKeywords, BaseMixin
from pds_pipelines.config import pds_log, pds_info, workarea, keyword_def, pds_db, upc_db, lock_obj, upc_error_queue, web_base, archive_base, recipe_base
from pds_pipelines.utils import generate_processes, process, add_process_db, get_isis_id


def getPDSid(infile):
    """
    Use ISIS to get the PDS Product ID of a cube.

    Using ISIS `getkey` is preferred over extracting the Product ID
    using the PVL library because of an edge case where PVL will
    erroneously convert Product IDs from string to floating point.

    Parameters
    ----------
    infile : str
             A string file path from which the Product ID will be extracted.


    Returns
    -------
    prod_id : str
        The PDS Product ID.
    """
    for key in ['product_id', 'productid']:
        try:
            prod_id = isis.getkey(from_=infile, keyword=key, recursive="TRUE")
            break
        except ProcessError as e:
            prod_id = None

    if not prod_id:
        return None

    # in later versions of ISIS, key values are returned as bytes
    if isinstance(prod_id, bytes):
        prod_id = prod_id.decode()
    prod_id = prod_id.replace('\n', '')
    return prod_id



def get_target_id(label, session_maker):
    """
    Fetches the target_id from the database. If a target_id is not found try
    to extract it from a given PDS label and add the target to the database

    Parameters
    ----------
    pds_label : Object
        Any type of pds object that can be indexed

    session_maker : sessionmaker
        sqlalchemy sessionmaker object for connection to and querying the
        database

    Returns
    -------
    target_id : int
        The defined target_id from the database. If this is 0 a target name
        could not be pulled from the label
    """
    try:
        target_name = label['TARGET_NAME']
    except KeyError as e:
        return None

    session = session_maker()
    target_qobj = session.query(Targets).filter(
        Targets.targetname == target_name.upper()).first()

    # If no matching table is found, create the entry in the database and
    #  access the new instance.
    if target_qobj is None:
        target_qobj = Targets.create(session, targetname=target_name,
                                              displayname=target_name.title(),
                                              system=target_name)
    target_id = target_qobj.targetid
    session.close()
    return target_id

def get_instrument_id(label, session_maker):
    """
    Fetches the instrument_id from the database. If a instrument_id is not
    found, try to extract it from a given PDS label and add the instrument to
    the database

    Parameters
    ----------
    label : Object
        Any type of pds object that can be indexed

    session_maker : sessionmaker
        sqlalchemy sessionmaker object for connection to and querying the
        database

    Returns
    -------
    instrument_id : int
        The defined instrument_id from the database. If this is 0 a instrument
        name could not be pulled from the label
    """
    # Although PDS3 the INSTRUMENT_NAME keyword, it is missing from some older datasets.
    #  PDS3 defines several (often interchangeable) keywords to
    #  hold instrument name, so try each of them in preferred order and grab the first match.
    # If no match is found, leave as None
    for inst in ['INSTRUMENT_NAME', 'INSTRUMENT_ID']:
        try:
            instrument_name = label[inst]
            break
        except KeyError as e:
            instrument_name = None

    if not instrument_name:
        return None

    # PDS3 does not require a keyword to hold spacecraft name,
    #  and PDS3 defines several (often interchangeable) keywords to
    #  hold spacecraft name, so try each of them in preferred order and grab the first match.
    # If no match is found, leave as None
    for sc in ['SPACECRAFT_NAME','INSTRUMENT_HOST_NAME','MISSION_NAME','SPACECRAFT_ID','INSTRUMENT_HOST_ID']:
        try:
            spacecraft_name = label[sc]
            break
        except KeyError:
            spacecraft_name = None

    if not spacecraft_name:
        return None

    session = session_maker()
    # Get the instrument from the instruments table.
    instrument_qobj = session.query(Instruments).filter(
        Instruments.instrument == instrument_name,
        Instruments.spacecraft == spacecraft_name).first()

    # If no matching instrument is found, create the entry in the database
    #  and access the new instance.
    if instrument_qobj is None:
        instrument_qobj = Instruments.create(session, instrument=instrument_name,
                                                      spacecraft=spacecraft_name)
    instrument_id = instrument_qobj.instrumentid
    session.close()
    return instrument_id

def read_json_footprint(footprint_file):
    with open(footprint_file, 'r') as fp:
        geo_json = json.load(fp)
        geo_str = json.dumps(geo_json['features'][0]['geometry'])

    footprint = ogr.CreateGeometryFromJson(geo_str)
    return footprint.ExportToWkt()

def create_datafiles_record(label, edr_source, input_cube, session_maker):
    """
    Creates a new DataFiles record through values from a given label and adds
    the new record to the database

    Parameters
    ----------
    label : Object
        Any type of pds object that can be indexed

    edr_source : str
        Path to the original label source

    input_cube : str
        Path to the cube generated from the data source

    session_maker : sessionmaker
        sqlalchemy sessionmaker object for connection to and querying the
        database

    Returns
    -------
    upc_id : int
        The defined upc_id from the database
    """
    try:
        # If there exists an array of values, then the first value is the
        #  path to the IMG.
        original_image_ext = os.path.splitext(label['^IMAGE'][0])[-1]
        img_file = os.path.splitext(edr_source)[0] + original_image_ext.lower()
        d_label = edr_source
    except (TypeError, KeyError):
        img_file = edr_source
        d_label = None

    datafile_attributes = dict.fromkeys(DataFiles.__table__.columns.keys(), None)

    datafile_attributes['source'] = img_file
    datafile_attributes['detached_label'] = d_label

    # Attempt to get the ISIS serial from the cube
    datafile_attributes['isisid'] = get_isis_id(input_cube)

    # Attempt to get the product id from the original label
    product_id = getPDSid(input_cube)

    datafile_attributes['productid'] = product_id
    datafile_attributes['instrumentid'] = get_instrument_id(label, session_maker)
    datafile_attributes['targetid'] = get_target_id(label, session_maker)

    session = session_maker()
    datafile_qobj = session.query(DataFiles).filter(
        DataFiles.source == img_file).first()

    if datafile_qobj is None:
        DataFiles.create(session, **datafile_attributes)
    else:
        datafile_attributes.pop('upcid')
        session.query(DataFiles).\
            filter(DataFiles.source == img_file).\
            update(datafile_attributes)
        session.commit()
    session.close()

    session = session_maker()
    upc_id = session.query(DataFiles).filter(
        DataFiles.source == img_file).first().upcid
    session.close()

    return upc_id

def create_search_terms_record(label, cam_info_pvl, upc_id, input_cube, footprint_file, search_term_mapping={}, session_maker=None):
    """
    Creates a new SearchTerms record through values from a given caminfo file
    and adds the new record to the database

    Parameters
    ----------
    cam_info_pvl : str
        Path to the caminfo output from the ISIS program caminfo

    upc_id : int
        upc id from the DataFiles record

    input_cube : str
        Path to the cube generated from the data source

    session_maker : sessionmaker
        sqlalchemy sessionmaker object for connection to and querying the
        database

    """
    search_term_attributes = dict.fromkeys(SearchTerms.__table__.columns.keys(), None)
    search_term_attributes['err_flag'] = True

    if not search_term_mapping:
        search_term_mapping = dict(zip(search_term_attributes.keys(), search_term_attributes.keys()))

    try:
        keywordsOBJ = UPCkeywords(cam_info_pvl)
    except:
        keywordsOBJ = None

    if keywordsOBJ:
        # For each key in the dictionary, get the related keyword from the keywords object
        for key in search_term_attributes.keys():
            try:
                search_term_attributes[key] = keywordsOBJ.getKeyword(search_term_mapping[key])
            except KeyError:
                search_term_attributes[key] = None

        search_term_attributes['isisfootprint'] = keywordsOBJ.getKeyword('GisFootprint')

        if os.path.exists(footprint_file):
            search_term_attributes['isisfootprint'] = read_json_footprint(footprint_file)

        search_term_attributes['err_flag'] = False

    search_term_attributes['upcid'] = upc_id

    product_id = getPDSid(input_cube)

    search_term_attributes['pdsproductid'] = product_id

    search_term_attributes['processdate'] = datetime.datetime.now(pytz.utc).strftime(
        "%Y-%m-%d %H:%M:%S")

    search_term_attributes['targetid'] = get_target_id(label, session_maker)
    search_term_attributes['instrumentid'] = get_instrument_id(label, session_maker)

    session = session_maker()
    search_terms_qobj = session.query(SearchTerms).filter(
        SearchTerms.upcid == upc_id).first()

    search_term_attributes = get_keyword_values(search_term_attributes)

    if search_terms_qobj is None:
        SearchTerms.create(session, **search_term_attributes)
    else:
        search_term_attributes.pop('upcid')
        session.query(SearchTerms).\
            filter(SearchTerms.upcid == upc_id).\
            update(search_term_attributes)
        session.commit()
    session.close()

def get_keyword_values(keywords):
    for k,v in keywords.items():
        try:
            keywords[k] = v.value
        except AttributeError:
            # Intentionally left blank.  If v doesn't have a .value, then it
            #  doesn't need to be converted.
            pass
    return keywords

def create_json_keywords_record(cam_info_pvl, upc_id, input_file, failing_command, session_maker, logger):
    """
    Creates a new SearchTerms record through values from a given caminfo file
    and adds the new record to the database

    Parameters
    ----------
    cam_info_pvl : str
        Path to the caminfo output from the ISIS program caminfo

    upc_id : int
        upc id from the DataFiles record

    input_file : str
        Path to the original data file being processed

    failing_command : str
        String presentation of the failing processing command

    session_maker : sessionmaker
        sqlalchemy sessionmaker object for connection to and querying the
        database

    """
    try:
        keywordsOBJ = UPCkeywords(cam_info_pvl)
    except Exception as e:
        logger.debug(f"Failed to create upc keywords with: {e}")
        keywordsOBJ = None

    json_keywords_attributes = dict.fromkeys(JsonKeywords.__table__.columns.keys(), None)

    json_keywords_attributes['upcid'] = upc_id

    try:
        json_keywords = json.dumps(keywordsOBJ.label, indent=4, sort_keys=True, default=str)
        json_keywords = json.loads(json_keywords)
    except Exception as e:
        logger.debug(f"Failed to load json keywords with: {e}")
        err_dict = {}
        err_dict['processdate'] = datetime.datetime.now(pytz.utc).strftime(
            "%Y-%m-%d %H:%M:%S")
        err_dict['errortype'] = failing_command
        err_dict['file'] = input_file
        err_dict['errormessage'] = f'Error running {failing_command} on file {input_file}'
        err_dict['error'] = True
        json_keywords = err_dict

    json_keywords_attributes['jsonkeywords'] = json_keywords

    session = session_maker()
    json_keywords_qobj = session.query(JsonKeywords).filter(
        JsonKeywords.upcid == upc_id).first()

    if json_keywords_qobj is None:
        JsonKeywords.create(session, **json_keywords_attributes)
    else:
        json_keywords_attributes.pop('upcid')
        session.query(JsonKeywords).\
            filter(JsonKeywords.upcid == upc_id).\
            update(json_keywords_attributes)
        session.commit()
    session.close()

def parse_args():
    parser = argparse.ArgumentParser(description='UPC Processing')
    parser.add_argument('--persist', '-p', dest="persist",
                        help="Keep intermediate .cub files.", action='store_true')
    parser.add_argument('--log', '-l', dest="log_level",
                        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'],
                        help="Set the log level.", default='INFO')
    parser.set_defaults(persist=False)
    args = parser.parse_args()
    return args

def main(user_args):
    upc_session_maker, upc_engine = db_connect(upc_db)
    pds_session_maker, pds_engine = db_connect(pds_db)

    persist = user_args.persist
    log_level = user_args.log_level

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

    # ***************** Set up logging *****************

    # Redis Queue Objects
    RQ_main = RedisQueue('UPC_ReadyQueue')
    logger.info("UPC Processing Queue: %s", RQ_main.id_name)

    RQ_error = RedisQueue(upc_error_queue)
    RQ_lock = RedisLock(lock_obj)
    # If the queue isn't registered, add it and set it to "running"
    RQ_lock.add({RQ_main.id_name: '1'})

    # if there are items in the redis queue
    if int(RQ_main.QueueSize()) > 0 and RQ_lock.available(RQ_main.id_name):
        # get a file from the queue
        item = literal_eval(RQ_main.QueueGet())
        inputfile = item[0]
        fid = item[1]
        archive = item[2]

        if not os.path.isfile(inputfile):
            RQ_error.QueueAdd(f'Unable to locate or access {inputfile} during UPC processing')
            logger.debug("%s is not a file\n", inputfile)
            exit()

        # Build URL for edr_source based on archive path from PDSinfo.json
        PDSinfoDICT = json.load(open(pds_info, 'r'))
        archive_path = PDSinfoDICT[archive]['path']
        orig_file = inputfile.replace(workarea, archive_path)
        edr_source = orig_file.replace(archive_base, web_base)

        # Update the logger context to include inputfile
        context['inputfile'] = inputfile

        recipe_file = recipe_base + "/" + archive + '.json'
        with open(recipe_file) as fp:
            upc_json = json.load(fp)['upc']
            recipe_string = json.dumps(upc_json['recipe'])
            # Attempt to get the optional search_term_mapping for the upc
            # process
            try:
                search_term_mapping = upc_json['search_term_mapping']
            except KeyError:
                search_term_mapping = {}

        no_extension_inputfile = os.path.splitext(inputfile)[0]
        cam_info_file = no_extension_inputfile + '_caminfo.pvl'
        footprint_file = no_extension_inputfile + '_footprint.json'

        processes = generate_processes(inputfile,
                                       recipe_string, logger,
                                       no_extension_inputfile=no_extension_inputfile,
                                       cam_info_file=cam_info_file,
                                       footprint_file=footprint_file)
        failing_command, _ = process(processes, workarea, logger)

        pds_label = pvl.load(inputfile)

        ######## Generate DataFiles Record ########
        upc_id = create_datafiles_record(pds_label, edr_source, no_extension_inputfile + '.cub', upc_session_maker)

        ######## Generate SearchTerms Record ########
        create_search_terms_record(pds_label, cam_info_file, upc_id, no_extension_inputfile + '.cub', footprint_file, search_term_mapping, upc_session_maker)

        ######## Generate JsonKeywords Record ########
        create_json_keywords_record(cam_info_file, upc_id, inputfile, failing_command, upc_session_maker, logger)

        try:
            pds_session = pds_session_maker()
            pds_session.flush()
        except:
            logger.debug("Unable to flush database connection")

        add_process_db(pds_session, fid, True)
        pds_session.close()

        if not persist:
            # Remove all files file from the workarea except for the copied
            # source file
            file_prefix = os.path.splitext(inputfile)[0]
            workarea_files = glob(file_prefix + '*')
            os.remove(os.path.join(workarea, 'print.prt'))
            for file in workarea_files:
                os.remove(file)

        # Disconnect from the engines
        pds_engine.dispose()
        upc_engine.dispose()

    logger.info("UPC processing exited")

if __name__ == "__main__":
    sys.exit(main(parse_args()))
