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

from pysis import isis
from pysis.exceptions import ProcessError
from pysis.isis import getsn

from pds_pipelines.RedisLock import RedisLock
from pds_pipelines.RedisQueue import RedisQueue
from pds_pipelines.Recipe import Recipe
from pds_pipelines.Process import Process
from pds_pipelines.UPCkeywords import UPCkeywords
from pds_pipelines.db import db_connect
from pds_pipelines.models import upc_models, pds_models
from pds_pipelines.models.upc_models import SearchTerms, Targets, Instruments, DataFiles, JsonKeywords
from pds_pipelines.config import pds_log, pds_info, workarea, keyword_def, pds_db, upc_db, lock_obj

from sqlalchemy import and_

def getISISid(infile):
    serial_num = getsn(from_=infile)
    # in later versions of getsn, serial_num is returned as bytes
    if isinstance(serial_num, bytes):
        serial_num = serial_num.decode()
    newisisSerial = serial_num.replace('\n', '')
    return newisisSerial


def find_keyword(obj, key):
    if key in obj:
        return obj[key]
    for _, v in obj.items():
        if isinstance(v, dict):
            F_item = find_keyword(v, key)
            if F_item is not None:
                return F_item

def db2py(key_type, value):
    """ Responsible for coercing database syntax to Python
        syntax (e.g. 'true' to True)

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


def main():
    logger = logging.getLogger('UPC_Process')
    logger.setLevel(logging.INFO)
    logFileHandle = logging.FileHandler(pds_log + 'Process.log')
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s, %(message)s')
    logFileHandle.setFormatter(formatter)
    logger.addHandler(logFileHandle)

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

    # @TODO Consider using a single error queue instead of UPC specific
    RQ_error = RedisQueue('UPC_ErrorQueue')
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
        if os.path.isfile(inputfile):
            pass
        else:
            print("{} is not a file\n".format(inputfile))
        if os.path.isfile(inputfile):
            logger.info('Starting Process: %s', inputfile)

            # @TODO refactor this logic.  We're using an object to find a path, returning it,
            #  then passing it back to the object so that the object can use it.
            recipeOBJ = Recipe()
            recipe_json = recipeOBJ.getRecipeJSON(archive)
            #recipe_json = recipeOBJ.getRecipeJSON(getMission(str(inputfile)))
            recipeOBJ.AddJsonFile(recipe_json, 'upc')

            infile = os.path.splitext(inputfile)[0] + '.UPCinput.cub'
            print(infile)
            outfile = os.path.splitext(inputfile)[0] + '.UPCoutput.cub'
            caminfoOUT= os.path.splitext(inputfile)[0] + '_caminfo.pvl'
            EDRsource = inputfile.replace(
                '/pds_san/PDS_Archive/',
                'https://pdsimage.wr.ugs.gov/Missions/')

            status = 'success'
            # Iterate through each process listed in the recipe
            for item in recipeOBJ.getProcesses():
                # If any of the processes failed, discontinue processing
                if status.lower() == 'error':
                    break
                elif status.lower() == 'success':
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
                            status = 'success'
                            if '2isis' in item:
                                label = pvl.load(infile)
                                infile_bandlist = label['IsisCube']['BandBin'][PDSinfoDICT[archive]['bandbinQuery']]
                                infile_centerlist = label['IsisCube']['BandBin']['Center']
                            elif item == 'thmproc':
                                pass
                            elif item == 'handmos':
                                label = pvl.load(infile)
                                infile_bandlist = label['IsisCube']['BandBin'][PDSinfoDICT[archive]['bandbinQuery']]
                                infile_centerlist = label['IsisCube']['BandBin']['Center']

                        except ProcessError as e:
                            print(e)
                            status = 'error'
                            processError = item

            # keyword definitions
            keywordsOBJ = None
            if status.lower() == 'success':
                try:
                    keywordsOBJ = UPCkeywords(caminfoOUT)
                except:
                    with open(caminfoOUT, 'r') as f:
                        filedata = f.read()

                    filedata = filedata.replace(';', '-').replace('&', '-')
                    filedata = re.sub(r'\-\s+', r'', filedata, flags=re.M)

                    with open(caminfoOUT, 'w') as f:
                        f.write(filedata)

                    keywordsOBJ = UPCkeywords(caminfoOUT)
                target_Qobj = session.query(upc_models.Targets).filter(
                    upc_models.Targets.targetname == keywordsOBJ.getKeyword(
                        'targetname').upper()).first()

                if target_Qobj is None:
                    target_input = upc_models.Targets(targetname=keywordsOBJ.getKeyword('target_name'),
                                                      displayname=keywordsOBJ.getKeyword('target'),
                                                      system=keywordsOBJ.getKeyword('target'))
                    session.merge(target_input)
                    session.commit()
                    target_Qobj = session.query(upc_models.Targets).filter(
                       upc_models.Targets.targetname == keywordsOBJ.getKeyword(
                           'targetname').upper()).first()


                instrument_Qobj = session.query(upc_models.Instruments).filter(
                    upc_models.Instruments.instrument == keywordsOBJ.getKeyword(
                        'InstrumentId')).first()
                if instrument_Qobj is None:
                    instrument_input = upc_models.Instruments(instrument=keywordsOBJ.getKeyword('instrumentid'),
                                                              spacecraft=keywordsOBJ.getKeyword('spacecraft_name'))
                    session.merge(instrument_input)
                    session.commit()
                    instrument_Qobj = session.query(upc_models.Instruments).filter(
		        upc_models.Instruments.instrument == keywordsOBJ.getKeyword('InstrumentId')).first()


                if session.query(upc_models.DataFiles).filter(
                        upc_models.DataFiles.isisid == keywordsOBJ.getKeyword(
                            'IsisId')).first() is None:

                    input_datafile = upc_models.DataFiles(isisid=keywordsOBJ.getKeyword('IsisId'),
                                                          productid=keywordsOBJ.getKeyword('ProductId'),
                                                          source=EDRsource,
                                                          detached_label='',
                                                          instrumentid=instrument_Qobj.instrumentid,
                                                          targetid=target_Qobj.targetid)

                    session.merge(input_datafile)
                    session.commit()

                Qobj = session.query(upc_models.DataFiles).filter(
                    upc_models.DataFiles.isisid == keywordsOBJ.getKeyword('IsisId')).first()

                UPCid = Qobj.upcid

                """
                bandlist = {}
                if isinstance(infile_bandlist, list):
                    bandlist = dict(zip(infile_bandlist, infile_centerlist))
                else:
                    try:
                        bandlist = {infile_bandlist: float(infile_centerlist[0])}
                    except TypeError:
                        bandlist = {infile_bandlist: float(infile_centerlist)}
                """

                keywordsOBJ.label.update(bandlist)

                # Create a dictionary with keys from the SearchTerms model
                attributes = dict.fromkeys(SearchTerms.__table__.columns.keys(), None)

                # For each key in the dictionary, get the related keyword from the keywords object
                for key in attributes:
                    try:
                        attributes[key] = keywordsOBJ.getKeyword(key)
                    except KeyError:
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
                db_input = upc_models.SearchTerms(**attributes)
                session.merge(db_input)

                # dictionary -> str -> dictionary for jsonb workaround. Converts datetime to serializable format
                json_keywords = json.dumps(keywordsOBJ.label, indent=4, sort_keys=True, default=str)
                json_keywords = json.loads(json_keywords)
                db_input = upc_models.JsonKeywords(upcid=attributes['upcid'], jsonkeywords=json_keywords)
                session.merge(db_input)

                try:
                    session.flush()
                except:
                    logger.warn("Unable to flush database connection")
                session.commit()

                AddProcessDB(pds_session, fid, True)
                os.remove(infile)
                os.remove(caminfoOUT)

            elif status.lower() == 'error':
                try:
                    label = pvl.load(infile)
                except Exception as e:
                    logger.info('%s', e)
                    # @TODO make sure that this is the right logic.
                    exit()
                err_dict = {}
                upc_id = None
                date = datetime.datetime.now(pytz.utc).strftime(
                    "%Y-%m-%d %H:%M:%S")

                if '2isis' in processError or processError == 'thmproc':
                    if session.query(upc_models.DataFiles).filter(
                            upc_models.DataFiles.source == EDRsource.decode(
                                "utf-8")).first() is None:

                        target_Qobj = session.query(upc_models.Targets).filter(
                            upc_models.Targets.targetname == str(
                                label['IsisCube']['Instrument']['TargetName']).upper()).first()

                        instrument_Qobj = session.query(upc_models.Instruments).filter(
                            upc_models.Instruments.instrument == str(
                                label['IsisCube']
                                ['Instrument']
                                ['InstrumentId'])).first()

                        error1_input = upc_models.DataFiles(isisid='1',
                                                            source=EDRsource)
                        session.merge(error1_input)
                        session.commit()

                    EQ1obj = session.query(upc_models.DataFiles).filter(
                        upc_models.DataFiles.source == EDRsource).first()
                    upc_id = EQ1obj.upcid

                    errorMSG = 'Error running {} on file {}'.format(
                        processError, inputfile)

                    err_dict['processdate'] = date
                    err_dict['errortype'] = processError
                    err_dict['errormessage'] = errorMSG
                    err_dict['error'] = True
                    err_dict['isisfootprint'] = 'POINT(361 0)'
                    err_dict['isiscentroid'] = 'POINT(361 0)'
                else:
                    try:
                        label = pvl.load(infile)
                    except Exception as e:
                        logger.warn('%s', e)
                        # @TODO make sure this is the right logic
                        exit()

                    isisSerial = getISISid(infile)

                    if session.query(upc_models.DataFiles).filter(
                            upc_models.DataFiles.isisid == isisSerial).first() is None:
                        target_Qobj = session.query(upc_models.Targets).filter(
                            upc_models.Targets.targetname == str(
                                label['IsisCube']['Instrument']['TargetName'])
                            .upper()).first()
                        instrument_Qobj = session.query(upc_models.Instruments).filter(
                            upc_models.Instruments.instrument == str(
                                label['IsisCube']
                                ['Instrument']
                                ['InstrumentId'])).first()

                        if target_Qobj is None or instrument_Qobj is None:
                            exit()

                        error2_input = upc_models.DataFiles(isisid=isisSerial,
                                                            productid=label['IsisCube']['Archive']['ProductId'],
                                                            source=EDRsource,
                                                            instrumentid=instrument_Qobj.instrumentid,
                                                            targetid=target_Qobj.targetid)
                        session.merge(error2_input)
                        session.commit()

                    try:
                        EQ2obj = session.query(upc_models.DataFiles).filter(
                            upc_models.DataFiles.isisid == isisSerial).first()
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
                    except:
                        pass

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
    logger.info("UPC processing exited successfully")

if __name__ == "__main__":
    sys.exit(main())
