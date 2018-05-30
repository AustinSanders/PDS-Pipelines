#!/usgs/apps/anaconda/bin/python
import os
import sys
import pvl
import subprocess
import datetime
import pytz
import logging
import json
import hashlib

from pysis import isis
from pysis.exceptions import ProcessError
from pysis.isis import getsn

from RedisQueue import *
from Recipe import *
from UPCkeywords import *
from db import db_connect
from models import upc_models, pds_models
from models.upc_models import MetaTime, MetaBands, MetaGeometry, MetaString, MetaBoolean

from sqlalchemy import *
from sqlalchemy.orm.util import *
from geoalchemy2 import Geometry
# from geoalchemy2.shape import to_shape

import pdb


def getMission(inputfile):
    inputfile = str(inputfile)
    if 'Mars_Reconnaissance_Orbiter/CTX' in inputfile:
        mission = 'mroCTX'
    elif 'USA_NASA_PDS_ODTSDP_100XX' in inputfile and 'odtie1' in inputfile:
        mission = 'themisIR_EDR'
    elif 'USA_NASA_PDS_ODTSDP_100XX' in inputfile and 'odtve1' in inputfile:
        mission = 'themisVIS_EDR'
    elif 'Lunar_Reconnaissance_Orbiter/LROC/EDR/' in inputfile:
        mission = 'lrolrcEDR'
    elif 'Cassini/ISS/' in inputfile:
        mission = 'cassiniISS'

    return mission


def getISISid(infile):
    serial_num = getsn(from_=infile)
    # in later versions of getsn, serial_num is returned as bytes
    if isistance(serial_num, bytes):
        serial_num = serial_num.decode()
    newisisSerial = serial_num.replace('\n', '')
    return newisisSerial


def find_keyword(obj, key):
    if key in obj:
        return obj[key]
    for k, v in obj.items():
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


def AddProcessDB(session, inputfile, outvalue):

    # pdb.set_trace()

    parts = inputfile.split("/")

    testfile = parts[-3] + "/" + parts[-2] + "/" + parts[-1]
    testfile2 = '%' + testfile + '%'

    fileQobj = session.query(pds_models.Files).filter(
        pds_models.Files.filename.like(testfile2)).first()

    if fileQobj is None:
        return 'ERROR'

    fileid = fileQobj.fileid

    date = datetime.datetime.now(pytz.utc).strftime("%Y-%m-%d %H:%M:%S")

    processDB = pds_models.ProcessRuns(fileid=fileQobj.fileid,
                             process_date=date,
                             process_typeid='5',
                             process_out=outvalue)

    try:
        session.add(processDB)
        session.commit()
        return 'SUCCESS'
    except:
        return 'ERROR'


def get_tid(keyword, session):
    tid = session.query(upc_models.Keywords.typeid).filter(upc_models.Keywords.typename == keyword).first()[0]
    print(tid)
    return tid
    


# @TODO set back to /usgs/ and /scratch/ directories.
def main():

    # pdb.set_trace()

    #workarea = '/scratch/pds_services/workarea/'
    workarea = '/home/arsanders/PDS-Pipelines/products/'

    # Connect to database - ignore engine information
    pds_session, _ = db_connect('pdsdi_dev')

    # Connect to database - ignore engine information
    session, _ = db_connect('upcdev')

    # ***************** Set up logging *****************
    logger = logging.getLogger('UPC_Process')
    logger.setLevel(logging.INFO)
    #logFileHandle = logging.FileHandler('/usgs/cdev/PDS/logs/Process.log')
    logFileHandle = logging.FileHandler('/home/arsanders/PDS-Pipelines/Process.log')
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s, %(message)s')
    logFileHandle.setFormatter(formatter)
    logger.addHandler(logFileHandle)

    PDSinfoDICT = json.load(open('/usgs/cdev/PDS/bin/PDSinfo.json', 'r'))

    # Redis Queue Objects
    RQ_main = RedisQueue('UPC_ReadyQueue')
    RQ_thumbnail = RedisQueue('Thumbnail_ReadyQueue')
    RQ_browse = RedisQueue('Browse_ReadyQueue')

    proc_date_tid = get_tid('processdate', session)
    err_type_tid = get_tid('errortype', session)
    err_msg_tid = get_tid('errormessage', session)
    err_flag_tid = get_tid('error', session)
    isis_footprint_id = get_tid('isisfootprint', session)
    isis_centroid_tid = get_tid('isiscentroid', session)
    start_time_tid = get_tid('starttime', session)
    stop_time_tid = get_tid('stoptime', session)

    # while there are items in the redis queue
    while int(RQ_main.QueueSize()) > 0:
        # get a file from the queue
        inputfile = (RQ_main.QueueGet()).decode('utf-8')
        if os.path.isfile(inputfile):
            pass
        else:
            print("{} is not a file\n".format(inputfile))
        if os.path.isfile(inputfile):
            logger.info('Starting Process: %s', inputfile)

            recipeOBJ = Recipe()
            recip_json = recipeOBJ.getRecipeJSON(getMission(str(inputfile)), 'upc')
            recipeOBJ.AddJsonFile(recip_json)

            infile = workarea + os.path.splitext(
                str(os.path.basename(inputfile)))[0] + '.UPCinput.cub'
            outfile = workarea + os.path.splitext(
                str(os.path.basename(inputfile)))[0] + '.UPCoutput.cub'
            caminfoOUT = workarea + os.path.splitext(
                str(os.path.basename(inputfile)))[0] + '_caminfo.pvl'
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
                    processR = processOBJ.ProcessFromRecipe(
                        item, recipeOBJ.getRecipe())

                    # Handle processing based on string description.
                    if '2isis' in item:
                        processOBJ.updateParameter('from_', inputfile)
                        processOBJ.updateParameter('to', outfile)
                    elif item == 'thmproc':
                        processOBJ.updateParameter('from_', inputfile)
                        processOBJ.updateParameter('to', outfile)
                        thmproc_odd = workarea
                        + os.path.splitext(os.path.basename(inputfile))[0]
                        + '.UPCoutput.raw.odd.cub'
                        thmproc_even = workarea
                        + os.path.splitext(os.path.basename(inputfile))[0]
                        + '.UPCoutput.raw.even.cub'
                    elif item == 'handmos':
                        processOBJ.updateParameter('from_', thmproc_even)
                        processOBJ.updateParameter('mosaic', thmproc_odd)
                    elif item == 'spiceinit':
                        processOBJ.updateParameter('from_', infile)
                    elif item == 'cubeatt':
                        exband = 'none'
                        for item1 in PDSinfoDICT[getMission(inputfile)]['bandorder']:
                            bandcount = 1
                            for item2 in label['IsisCube']['BandBin'][PDSinfoDICT[getMission(inputfile)]['bandbinQuery']]:
                                if str(item1) == str(item2):
                                    exband = bandcount
                                    break
                                bandcount = bandcount + 1
                            if exband != 'none':
                                break

                        band_infile = infile + '+' + str(exband)
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

                    # iterate through functions listed in process obj
                    for k, v in processOBJ.getProcess().items():
                        # load a function into func
                        func = getattr(isis, k)
                        try:
                            # execute function
                            func(**v)
                            if item == 'handmos':
                                if os.path.isfile(thmproc_odd):
                                    os.rename(thmproc_odd, infile)
                            else:
                                if os.path.isfile(outfile):
                                    os.rename(outfile, infile)
                            status = 'success'
                            if '2isis' in item:
                                RQ_thumbnail.QueueAdd(inputfile)
                                RQ_browse.QueueAdd(inputfile)
                                label = pvl.load(infile)
                                infile_bandlist = label['IsisCube']['BandBin'][PDSinfoDICT[getMission(
                                    inputfile)]['bandbinQuery']]
                                infile_centerlist = label['IsisCube']['BandBin']['Center']
                            elif item == 'thmproc':
                                RQ_thumbnail.QueueAdd(inputfile)
                                RQ_browse.QueueAdd(inputfile)
                            elif item == 'handmos':
                                label = pvl.load(infile)
                                infile_bandlist = label['IsisCube']['BandBin'][PDSinfoDICT[getMission(
                                    inputfile)]['bandbinQuery']]
                                infile_centerlist = label['IsisCube']['BandBin']['Center']

                        except ProcessError as e:
                            print(e)
                            status = 'error'
                            processError = item

            # keyword definitions
            keywordsOBJ = None
            if status.lower() == 'success':
                keywordsOBJ = UPCkeywords(caminfoOUT)
                if session.query(upc_models.DataFiles).filter(
                        upc_models.DataFiles.isisid == keywordsOBJ.getKeyword(
                            'Parameters', 'IsisId')).first() == None:

                    target_Qobj = session.query(upc_models.Targets).filter(
                        upc_models.Targets.targetname == keywordsOBJ.getKeyword(
                            'Instrument', 'TargetName').upper()).first()

                    instrument_Qobj = session.query(upc_models.Instruments).filter(
                        upc_models.Instruments.instrument == keywordsOBJ.getKeyword(
                            'Instrument', 'InstrumentId')).first()

                    PVL_productid = keywordsOBJ.getKeyword(
                        'Archive', 'ProductId')

                    test_input = upc_models.DataFiles(
                        isisid=keywordsOBJ.getKeyword('Parameters', 'IsisId'),
                        productid=keywordsOBJ.getKeyword(
                            'Archive', 'ProductId'),
                        edr_source=EDRsource,
                        edr_detached_label='',
                        instrumentid=instrument_Qobj.instrumentid,
                        targetid=target_Qobj.targetid)

                    session.add(test_input)
                    session.commit()

                Qobj = session.query(upc_models.DataFiles).filter(
                    upc_models.DataFiles.isisid == keywordsOBJ.getKeyword(
                        'Parameters', 'IsisId')).first()

                UPCid = Qobj.upcid
# block to add band information to meta_bands
                if type(infile_bandlist) == list:
                    index = 0
                    while index < len(infile_bandlist):
                        B_DBinput = upc_models.MetaBands(upcid=UPCid, filter=infile_bandlist[index], centerwave=infile_centerlist[index])
                        session.add(B_DBinput)
                        index = index + 1
                else:
                    B_DBinput = upc_models.MetaBands(upcid=UPCid, filter=infile_bandlist, centerwave=float(infile_centerlist[0]))
                    session.add(B_DBinput)
                session.commit()

#  Block to add common keywords
                testjson = json.load(
                    open('/usgs/cdev/PDS/recipe/Keyword_Definition.json', 'r'))
                for element_1 in testjson['instrument']['COMMON']:
                    keyvalue = ""
                    keytype = testjson['instrument']['COMMON'][element_1]['type']
                    keygroup = testjson['instrument']['COMMON'][element_1]['group']
                    keyword = testjson['instrument']['COMMON'][element_1]['keyword']
                    keyword_Qobj = session.query(
                        upc_models.Keywords).filter(upc_models.Keywords.typename == element_1).first()
                    if keygroup == 'Polygon':
                        keyvalue = keywordsOBJ.getPolygonKeyword(keyword)
                    else:
                        keyvalue = keywordsOBJ.getKeyword(keygroup, keyword)
                    keyvalue = db2py(keytype, keyvalue)
                    DBinput = upc_models.create_table(keytype,
                                                  upcid=UPCid,
                                                  typeid=keyword_Qobj.typeid,
                                                  value=keyvalue)

                    session.add(DBinput)
                session.commit()

                # geometry stuff
                G_centroid = 'point ({} {})'.format(
                    str(keywordsOBJ.getKeyword(
                        'Polygon', 'CentroidLongitude')),
                    str(keywordsOBJ.getKeyword(
                        'Polygon', 'CentroidLatitude')))

                G_keyword_Qobj = session.query(upc_models.Keywords).filter(
                    upc_models.Keywords.typename == 'isiscentroid').first()
                G_DBinput = upc_models.MetaGeometry(upcid=UPCid, typeid=G_keyword_Qobj.typeid, value=G_centroid)
                session.add(G_DBinput)

                G_footprint = keywordsOBJ.getKeyword('Polygon', 'GisFootprint')
                G_footprint_Qobj = session.query(upc_models.Keywords).filter(
                    upc_models.Keywords.typename == 'isisfootprint').first()
                G_DBinput = upc_models.MetaGeometry(upcid=UPCid, typeid=G_footprint_Qobj.typeid, value=G_footprint)
                session.add(G_DBinput)
                session.commit()

                # block to deal with mission keywords
                for element_2 in testjson['instrument'][getMission(inputfile)]:
                    M_keytype = testjson['instrument'][getMission(
                        inputfile)][element_2]['type']
                    M_keygroup = testjson['instrument'][getMission(
                        inputfile)][element_2]['group']
                    M_keyword = testjson['instrument'][getMission(
                        inputfile)][element_2]['keyword']

                    M_keyword_Qobj = session.query(upc_models.Keywords).filter(
                        upc_models.Keywords.typename == element_2).first()

                    if M_keygroup == 'Polygon':
                        M_keyvalue = keywordsOBJ.getPolygonKeyword(M_keyword)
                    else:
                        M_keyvalue = keywordsOBJ.getKeyword(
                            M_keygroup, M_keyword)

                    M_keyvalue = db2py(M_keytype, M_keyvalue)

                    DBinput = upc_models.create_table(M_keytype,
                                                  upcid=UPCid,
                                                  typeid=M_keyword_Qobj.typeid,
                                                  value=M_keyvalue)

                    session.add(DBinput)
                session.commit()

                """
                CScmd = 'md5sum ' + inputfile
                process = subprocess.Popen(CScmd,
                                           stdout=subprocess.PIPE, shell=True)
                (stdout, stderr) = process.communicate()
                checksum = stdout.split()[0]
                """

                f_hash = hashlib.md5()
                with open(inputfile, "rb") as f:
                    for chunk in iter(lambda: f.read(4096), b""):
                        f_hash.update(chunk)
                checksum = f_hash.hexdigest()


                # set typeid to 5 on the prod DB - 101 for dev
                # production typeid 5 = checksum
                # dev typeid 101 = checksum
                DBinput = upc_models.MetaString(upcid=UPCid, typeid=101, value=checksum)
                session.add(DBinput)
                # add error keyword to UPC
                # typeid 595 = error flag
                DBinput = upc_models.MetaBoolean(upcid=UPCid, typeid=595, value=False)
                session.add(DBinput)
                session.commit()
                AddProcessDB(pds_session, inputfile, True)
                os.remove(infile)
                os.remove(caminfoOUT)

            elif status.lower() == 'error':
                label = pvl.load(infile)
                date = datetime.datetime.now(pytz.utc).strftime(
                    "%Y-%m-%d %H:%M:%S")

                print(processError)
                if '2isis' in processError or processError == 'thmproc':

                    testspacecraft = PDSinfoDICT[getMission(
                        inputfile)]['UPCerrorSpacecraft']
                    testinst = PDSinfoDICT[getMission(
                        inputfile)]['UPCerrorInstrument']
                    if session.query(upc_models.DataFiles).filter(
                            upc_models.DataFiles.edr_source == EDRsource.decode(
                                "utf-8")).first() == None:

                        target_Qobj = session.query(upc_models.Targets).filter(upc_models.Targets.targetname == str(label['IsisCube']['Instrument']['TargetName']) .upper()).first()

                        instrument_Qobj = session.query(upc_models.Instruments).filter(
                            upc_models.Instruments.instrument == str(
                                label['IsisCube']
                                ['Instrument']
                                ['InstrumentId'])).first()

                        error1_input = upc_models.DataFiles(isisid='1',
                                                            edr_source=EDRsource)
                        session.add(error1_input)
                        session.commit()

                    EQ1obj = session.query(upc_models.DataFiles).filter(
                        upc_models.DataFiles.edr_source == EDRsource).first()
                    UPCid = EQ1obj.upcid

                    errorMSG = 'Error running {} on file {}'.format(
                        processError, inputfile)

                    DBinput = MetaTime(upcid=UPCid,
                                       typeid=proc_date_tid,
                                       value=date)
                    session.add(DBinput)

                    DBinput = MetaString(upcid=UPCid,
                                         typeid=err_type_tid,
                                         value=processError)
                    session.add(DBinput)

                    DBinput = MetaString(upcid=UPCid,
                                         typeid=err_msg_tid,
                                         value=errorMSG)
                    session.add(DBinput)

                    DBinput = MetaBoolean(upcid=UPCid,
                                          typeid=err_flag_tid,
                                          value='true')
                    session.add(DBinput)

                    DBinput = MetaGeometry(upcid=UPCid,
                                           typeid=isis_footprint_tid,
                                           value='POINT(361 0)')
                    session.add(DBinput)

                    DBinput = MetaGeometry(upcid=UPCid,
                                           typeid=isis_centroid_tid,
                                           value='POINT(361 0)')
                    session.add(DBinput)

                    session.commit()

                else:
                    label = pvl.load(infile)

                    isisSerial = getISISid(infile)

                    if session.query(upc_models.DataFiles).filter(
                            upc_models.DataFiles.isisid == isisSerial).first() == None:
                        target_Qobj = session.query(upc_models.Targets).filter(
                            upc_models.Targets.targetname == str(
                                label['IsisCube']['Instrument']['TargetName'])
                            .upper()).first()
                        instrument_Qobj = session.query(upc_models.Instruments).filter(
                            upc_models.Instruments.instrument == str(
                                label['IsisCube']
                                ['Instrument']
                                ['InstrumentId'])).first()

                        error2_input = upc_models.DataFiles(isisid=isisSerial,
                                                 productid=label['IsisCube']['Archive']['ProductId'],
                                                 edr_source=EDRsource,
                                                 instrumentid=instrument_Qobj.instrumentid,
                                                 targetid=target_Qobj.targetid)
                    session.add(error2_input)
                    session.commit()

                    EQ2obj = session.query(upc_models.DataFiles).filter(
                        upc_models.DataFiles.isisid == isisSerial).first()
                    UPCid = EQ2obj.upcid
                    errorMSG = 'Error running {} on file {}'.format(
                        processError, inputfile)

                    DBinput = MetaTime(upcid=UPCid,
                                       typeid=proc_date_tid,
                                       value=date)
                    session.add(DBinput)

                    DBinput = MetaString(upcid=UPCid,
                                         typeid=err_type_tid,
                                         value=processError)
                    session.add(DBinput)

                    DBinput = MetaString(upcid=UPCid,
                                         typeid=err_msg_tid,
                                         value=errorMSG)
                    session.add(DBinput)

                    DBinput = MetaBoolean(upcid=UPCid,
                                          typeid=err_flag_tid,
                                          value='true')
                    session.add(DBinput)

                    DBinput = MetaGeometry(upcid=UPCid,
                                           typeid=isis_footprint_tid,
                                           value='POINT(361 0)')
                    session.add(DBinput)

                    DBinput = MetaGeometry(upcid=UPCid,
                                           typeid=isis_centroid_tid,
                                           value='POINT(361 0)')
                    session.add(DBinput)

                    DBinput = MetaTime(upcid=UPCid,
                                       typeid=star_time_tid,
                                       value=label['IsisCube']['Instrument']['StartTime'])
                    session.add(DBinput)

                    DBinput = MetaTime(upcid=UPCid,
                                       typeid=stop_time_tid,
                                       value=label['IsisCube']['Instrument']['StopTime'])
                    session.add(DBinput)

                    session.commit()

                AddProcessDB(pds_session, inputfile, 'f')
                os.remove(infile)


if __name__ == "__main__":
    sys.exit(main())
