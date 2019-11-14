#!/usr/bin/env python
import re
import os
import sys
import datetime
import logging
import hashlib
import json
from ast import literal_eval
import pytz
import pvl
import argparse

from pysis import isis
from pysis.exceptions import ProcessError
from pysis.isis import getsn, getkey

from pds_pipelines.RedisLock import RedisLock
from pds_pipelines.RedisQueue import RedisQueue
from pds_pipelines.Recipe import Recipe
from pds_pipelines.Process import Process
from pds_pipelines.UPCkeywords import UPCkeywords
from pds_pipelines.db import db_connect
from pds_pipelines.models import pds_models
from pds_pipelines.models.upc_models import SearchTerms, Targets, Instruments, DataFiles, JsonKeywords
from pds_pipelines.config import pds_log, pds_info, workarea, keyword_def, pds_db, upc_db, lock_obj, upc_error_queue, web_base

from sqlalchemy import and_

def getPDSid(infile):
    """ Use ISIS to get the PDS Product ID of a cube.

        Using ISIS `getkey` is preferred over extracting the Product ID 
        using the PVL library because of an edge case where PVL will
        erroneously convert Product IDs from string to floating point. 

    Parameters
    ----------
    infile : str
        A string file path from which the Product ID will be extracted.


    Returns
    -------
    str
        The PDS Product ID.
    """
    prod_id = getkey(from_=infile, keyword="Product_Id", grp="Archive")
    # in later versions of ISIS, key values are returned as bytes
    if isinstance(prod_id, bytes):
        prod_id = prod_id.decode()
    prod_id = prod_id.replace('\n', '')
    return prod_id


def getISISid(infile):
    """ Use ISIS to get the serial number of a file.

    Parameters
    ----------
    infile : str
        A string file path for which the serial number will be calculated.


    Returns
    -------
    str
        The serial number of the input file.
    """
    serial_num = getsn(from_=infile)
    # in later versions of getsn, serial_num is returned as bytes
    if isinstance(serial_num, bytes):
        serial_num = serial_num.decode()
    newisisSerial = serial_num.replace('\n', '')
    return newisisSerial


def db2py(key_type, value):
    """ Coerce database syntax to Python syntax (e.g. 'true' to True)

    Parameters
    ----------
    key_type : str
        A string type description of the value.
    value : obj
        The value of the object being coerced to Python syntax

    Returns
    -------
    out : obj
        A Python-syntax value based on the keytype and value pair.
    """

    if key_type == "double":
        if isinstance(value, pvl.Units):
            # pvl.Units(value=x, units=y), we are only interested in value
            value = value.value
        return value
    elif key_type == "boolean":
        return (str(value).lower() == "true")
    else:
        return value


def AddProcessDB(session, fid, outvalue):
    """ Add a process run to the database.

    Parameters
    ----------
    session : Session
        The database session to which the process will be added.

    fid : str
        The file id.

    outvalue : str
        The return value / output of the process that will be added to the database.

    Returns
    -------
    str :
        "SUCCESS" on success, "ERROR" on failure

    """

    # pdb.set_trace()
    date = datetime.datetime.now(pytz.utc).strftime("%Y-%m-%d %H:%M:%S")

    processDB = pds_models.ProcessRuns(fileid=fid,
                                       process_date=date,
                                       process_typeid='5',
                                       process_out=outvalue)

    try:
        session.merge(processDB)
        session.commit()
        return 'SUCCESS'
    except:
        return 'ERROR'


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


def main(persist, log_level):
    try:
        slurm_job_id = os.environ['SLURM_ARRAY_JOB_ID']
        slurm_array_id = os.environ['SLURM_ARRAY_TASK_ID']
    except:
        slurm_job_id = ''
        slurm_array_id = ''
    inputfile = ''
    context = {'job_id': slurm_job_id, 'array_id':slurm_array_id,'inputfile':inputfile}
    logger = logging.getLogger('UPC_Process')
    level = logging.getLevelName(log_level)
    logger.setLevel(level)
    logFileHandle = logging.FileHandler(pds_log + 'Process.log')
    formatter = logging.Formatter(
        '%(asctime)s - %(job_id)s - %(array_id)s - %(inputfile)s - %(name)s - %(levelname)s, %(message)s')
    logFileHandle.setFormatter(formatter)
    logger.addHandler(logFileHandle)
    logger = logging.LoggerAdapter(logger, context)

    try:
        # Connect to database - ignore engine information
        pds_session, pds_engine = db_connect(pds_db)

        # Connect to database - ignore engine information
        session, upc_engine = db_connect(upc_db)
    except Exception as e:
        logger.error('Unable to connect to database: %s', e)


    # ***************** Set up logging *****************

    PDSinfoDICT = json.load(open(pds_info, 'r'))

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
            logger.warn("%s is not a file\n", inputfile)
            pds_session.close()
            session.close()
            pds_engine.dispose()
            upc_engine.dispose()
            exit()
        logger.info('Starting Process: %s', inputfile)

        # Update the logger context to include inputfile
        context['inputfile'] = inputfile

        # @TODO refactor this logic.  We're using an object to find a path, returning it,
        #  then passing it back to the object so that the object can use it.
        recipeOBJ = Recipe()
        recipe_json = recipeOBJ.getRecipeJSON(archive)
        #recipe_json = recipeOBJ.getRecipeJSON(getMission(str(inputfile)))
        recipeOBJ.AddJsonFile(recipe_json, 'upc')

        infile = os.path.splitext(inputfile)[0] + '.UPCinput.cub'
        logger.debug("Beginning processing on %s\n", inputfile)
        outfile = os.path.splitext(inputfile)[0] + '.UPCoutput.cub'
        caminfoOUT= os.path.splitext(inputfile)[0] + '_caminfo.pvl'
        EDRsource = inputfile.replace(workarea, web_base)

        status = 'success'
        # Iterate through each process listed in the recipe
        for item in recipeOBJ.getProcesses():
            # If any of the processes failed, discontinue processing
            if status == 'error':
                break
            elif status == 'success':
                processOBJ = Process()
                processOBJ.ProcessFromRecipe(item, recipeOBJ.getRecipe())
                # Handle processing based on string description.
                if '2isis' in item:
                    processOBJ.updateParameter('from_', inputfile)
                    processOBJ.updateParameter('to', outfile)
                elif item == 'thmproc':
                    processOBJ.updateParameter('from_', inputfile)
                    processOBJ.updateParameter('to', outfile)
                    thmproc_odd = str(workarea) + str(os.path.splitext(
                        os.path.basename(inputfile))[0]) + '.UPCoutput.raw.odd.cub'
                    thmproc_even = str(workarea) + str(
                        os.path.splitext(os.path.basename(
                            inputfile))[0]) + '.UPCoutput.raw.even.cub'
                elif item == 'handmos':
                    processOBJ.updateParameter('from_', thmproc_even)
                    processOBJ.updateParameter('mosaic', thmproc_odd)
                elif item == 'spiceinit':
                    processOBJ.updateParameter('from_', infile)
                elif item == 'cubeatt':
                    band_infile = infile + '+' + str(1)
                    processOBJ.updateParameter('from_', band_infile)
                    processOBJ.updateParameter('to', outfile)
                elif item == 'footprintinit':
                    processOBJ.updateParameter('from_', infile)
                elif item == 'caminfo':
                    processOBJ.updateParameter('from_', infile)
                    processOBJ.updateParameter('to', caminfoOUT)
                else:
                    processOBJ.updateParameter('from_', infile)
                    processOBJ.updateParameter('to', outfile)

                pwd = os.getcwd()
                # iterate through functions listed in process obj
                for k, v in processOBJ.getProcess().items():
                    # load a function into func
                    func = getattr(isis, k)
                    try:
                        os.chdir(workarea)
                        # execute function
                        func(**v)
                        os.chdir(pwd)
                        if item == 'handmos':
                            if os.path.isfile(thmproc_odd):
                                os.rename(thmproc_odd, infile)
                        else:
                            if os.path.isfile(outfile):
                                os.rename(outfile, infile)

                    except ProcessError as e:
                        logger.error("%s", e)
                        status = 'error'
                        processError = item

        pds_label = pvl.load(inputfile)

        try:
            # If there exists an array of values, then the first value is the
            #  path to the IMG.
            img_file = pds_label['^IMAGE'][0]
            d_label = EDRsource
        except TypeError:
            img_file = EDRsource
            d_label = None

        # get the target from the targets table.
        target_Qobj = session.query(Targets).filter(
            Targets.targetname == pds_label['TARGET_NAME'].upper()).first()

        # If no matching table is found, create the entry in the database and
        #  access the new instance.
        if target_Qobj is None:
            target_input = Targets(targetname=pds_label['TARGET_NAME'],
                                   displayname=pds_label['TARGET_NAME'].title(),
                                   system=pds_label['TARGET_NAME'])
            session.merge(target_input)
            session.commit()
            target_Qobj = session.query(Targets).filter(Targets.targetname == pds_label['TARGET_NAME']).first()

        # Get the instrument from the instruments table.
        instrument_Qobj = session.query(Instruments).filter(
            Instruments.instrument == pds_label['INSTRUMENT_ID'],
            Instruments.spacecraft == pds_label['SPACECRAFT_NAME']).first()

        # If no matching instrument is found, create the entry in the database
        #  and access the new instance.
        if instrument_Qobj is None:
            instrument_input = Instruments(instrument=pds_label['INSTRUMENT_ID'],
                                           spacecraft=pds_label['SPACECRAFT_NAME'])
            session.merge(instrument_input)
            session.commit()
            instrument_Qobj = session.query(Instruments).filter(
                Instruments.instrument == pds_label['INSTRUMENT_ID'],
                Instruments.spacecraft == pds_label['SPACECRAFT_NAME']).first()

        # keyword definitions
        keywordsOBJ = None
        if status == 'success':
            try:
                keywordsOBJ = UPCkeywords(caminfoOUT)
            except:
                # Some labels are poorly formatted or include characters that
                #  cannot be parsed with the PVL library.  This finds those
                #  characters and replaces them so that we can properly parse
                #  the PVL.
                with open(caminfoOUT, 'r') as f:
                    filedata = f.read()

                filedata = filedata.replace(';', '-').replace('&', '-')
                filedata = re.sub(r'\-\s+', r'', filedata, flags=re.M)

                with open(caminfoOUT, 'w') as f:
                    f.write(filedata)

                keywordsOBJ = UPCkeywords(caminfoOUT)

            input_datafile = DataFiles(isisid=keywordsOBJ.getKeyword('IsisId'),
                                                  productid=getPDSid(caminfoOUT),
                                                  source=img_file,
                                                  detached_label=d_label,
                                                  instrumentid=instrument_Qobj.instrumentid,
                                                  targetid=target_Qobj.targetid)

            session.merge(input_datafile)
            session.commit()

            Qobj = session.query(DataFiles).filter( DataFiles.source==img_file).first()
            UPCid = Qobj.upcid

            # Create a dictionary with keys from the SearchTerms model
            attributes = dict.fromkeys(SearchTerms.__table__.columns.keys(), None)

            # For each key in the dictionary, get the related keyword from the keywords object
            for key in attributes:
                try:
                    attributes[key] = keywordsOBJ.getKeyword(key)
                except KeyError:
                    attributes[key] = None
                    logger.warn("Unable to find key '%s' in keywords object", key)

            attributes['upctime'] = datetime.datetime.now(pytz.utc).strftime(
                "%Y-%m-%d %H:%M:%S")

            # Calculate checksum and store in JSON
            f_hash = hashlib.md5()
            with open(inputfile, "rb") as f:
                for chunk in iter(lambda: f.read(4096), b""):
                    f_hash.update(chunk)
            checksum = f_hash.hexdigest()
            keywordsOBJ.label['checksum'] = checksum

            attributes['isisfootprint'] = keywordsOBJ.getKeyword('GisFootprint')
            attributes['err_flag'] = False

            attributes['targetid'] = target_Qobj.targetid
            attributes['instrumentid'] = instrument_Qobj.instrumentid
            db_input = SearchTerms(**attributes)
            session.merge(db_input)

            # dictionary -> str -> dictionary for jsonb workaround. Converts datetime to serializable format
            json_keywords = json.dumps(keywordsOBJ.label, indent=4, sort_keys=True, default=str)
            json_keywords = json.loads(json_keywords)
            db_input = JsonKeywords(upcid=attributes['upcid'], jsonkeywords=json_keywords)
            session.merge(db_input)

            try:
                session.flush()
            except:
                logger.warn("Unable to flush database connection")
            session.commit()

            AddProcessDB(pds_session, fid, True)

            if not persist:
                os.remove(infile)
                os.remove(caminfoOUT)

        elif status == 'error':
            try:
                label = pvl.load(infile)
            except Exception as e:
                logger.error('%s', e)
                exit()
            err_dict = {}
            upc_id = None
            date = datetime.datetime.now(pytz.utc).strftime(
                "%Y-%m-%d %H:%M:%S")

            if '2isis' in processError or processError == 'thmproc':
                if session.query(DataFiles).filter(
                        DataFiles.source == img_file.decode(
                            "utf-8")).first() is None:

                    error_input = DataFiles(isisid='1', source=img_file)
                    session.merge(error_input)
                    session.commit()

                EQ1obj = session.query(DataFiles).filter(
                    DataFiles.source == img_file).first()
                upc_id = EQ1obj.upcid

                errorMSG = 'Error running {} on file {}'.format(
                    processError, inputfile)

                err_dict['processdate'] = date
                err_dict['errortype'] = processError
                err_dict['errormessage'] = errorMSG
                err_dict['error'] = True
            else:
                try:
                    label = pvl.load(infile)
                except Exception as e:
                    logger.error('%s', e)
                    exit()

                isisSerial = getISISid(infile)
                PDSid = getPDSid(infile)

                error_input = DataFiles(isisid=isisSerial,
                                        productid=PDSid,
                                        source=img_file,
                                        instrumentid=instrument_Qobj.instrumentid,
                                        targetid=target_Qobj.targetid)
                session.merge(error_input)
                session.commit()

                try:
                    EQ2obj = session.query(DataFiles).filter(
                        DataFiles.isisid == isisSerial).first()
                    upc_id = EQ2obj.upcid
                    errorMSG = 'Error running {} on file {}'.format(
                        processError, inputfile)
                    err_dict['processdate'] = date
                    err_dict['errortype'] = processError
                    err_dict['file'] = inputfile
                    err_dict['errormessage'] = errorMSG
                    err_dict['error'] = True
                    logger.warn('%s', errorMSG)
                    RQ_error.QueueAdd((inputfile, processError))
                except Exception as e:
                    logger.warn('%s', e)

            db_input = SearchTerms(upcid=upc_id, upctime=date, err_flag=True)
            session.merge(db_input)

            db_input = JsonKeywords(upcid=upc_id, jsonkeywords=err_dict)
            session.merge(db_input)
            session.commit()

            AddProcessDB(pds_session, fid, False)
            os.remove(infile)

    # Disconnect from db sessions
    pds_session.close()
    session.close()
    # Disconnect from the engines
    pds_engine.dispose()
    upc_engine.dispose()

    logger.info("UPC processing exited")

if __name__ == "__main__":
    args = parse_args()
    sys.exit(main(**vars(args)))
