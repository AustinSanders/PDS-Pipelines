#!/usgs/apps/anaconda/bin/python
import os
import sys
import pvl
import subprocess
import datetime
import pytz
import logging
import json

from pysis import isis
from pysis.exceptions import ProcessError
from pysis.isis import getsn

from RedisQueue import *
from Recipe import *
from UPCkeywords import *
from UPC_test import *
from db import db_connect
from models import upc_models, pds_models

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

    SerialNum = getsn(from_=infile)
    newisisSerial = SerialNum.replace('\n', '')
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


def main():

    # pdb.set_trace()

    #workarea = '/scratch/pds_services/workarea/'
    workarea = '/home/arsanders/PDS-Pipelines/products/'

    pds_session, pds_engine = db_connect('pdsdi_dev')

    # Connect to database - ignore archive and volume information
    session, engine = db_connect('upcdev')

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
                if status == 'error':
                    break
                elif status == 'success':
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
                            print('test of item1: {}'.format(item1))
                            bandcount = 1
                            for item2 in label['IsisCube']['BandBin'][PDSinfoDICT[getMission(inputfile)]['bandbinQuery']]:
                                print('test of item2: {}'.format(item2))
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

                    # test of process
                    print(processOBJ.getProcess())

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
            if status == 'success':
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
                    print('Test of productid: {}'.format(PVL_productid))

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
                        print('test of filter: {}'.format(
                              str(infile_bandlist[index])))
                        print('test of center: {}'.format(
                              str(infile_centerlist[index])))
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
                    print('Inside element_1 test: {}'.format(element_1))
                    keytype = testjson['instrument']['COMMON'][element_1]['type']
                    print('test of keytype: {}'.format(keytype))
                    keygroup = testjson['instrument']['COMMON'][element_1]['group']
                    print('test of keygroup: {}'.format(keygroup))
                    keyword = testjson['instrument']['COMMON'][element_1]['keyword']
                    print('test of keyword: {}'.format(keyword))

                    keyword_Qobj = session.query(
                        upc_models.Keywords).filter(upc_models.Keywords.typename == element_1).first()
                    print('test of keyword typeid: {}'.format(str(
                        keyword_Qobj.typeid)))

                    if keygroup == 'Polygon':
                        print('Polygon Keyword')
                        keyvalue = keywordsOBJ.getPolygonKeyword(keyword)
                        print(keyvalue)
                    else:
                        keyvalue = keywordsOBJ.getKeyword(keygroup, keyword)
                        print(keyvalue)
                    print('test of keyvalue: {}'.format(keyvalue))
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
                    print('Inside element_2 test: {}'.format(element_2))
                    M_keytype = testjson['instrument'][getMission(
                        inputfile)][element_2]['type']
                    M_keygroup = testjson['instrument'][getMission(
                        inputfile)][element_2]['group']
                    M_keyword = testjson['instrument'][getMission(
                        inputfile)][element_2]['keyword']

                    M_keyword_Qobj = session.query(upc_models.Keywords).filter(
                        upc_models.Keywords.typename == element_2).first()

                    if M_keygroup == 'Polygon':
                        print('Polygon Keyword')
                        M_keyvalue = keywordsOBJ.getPolygonKeyword(M_keyword)
                    else:
                        M_keyvalue = keywordsOBJ.getKeyword(
                            M_keygroup, M_keyword)

                    M_keyvalue = db2py(M_keytype, M_keyvalue)

                    print('Mission keyvalue is: {}'.format(M_keyvalue))

                    DBinput = upc_models.create_table(M_keytype,
                                                  upcid=UPCid,
                                                  typeid=M_keyword_Qobj.typeid,
                                                  value=M_keyvalue)

                    session.add(DBinput)
                session.commit()

                CScmd = 'md5sum ' + inputfile
                process = subprocess.Popen(CScmd,
                                           stdout=subprocess.PIPE, shell=True)
                (stdout, stderr) = process.communicate()
                checksum = stdout.split()[0]

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
                AddProcessDB(pds_session, inputfile, 't')
                os.remove(infile)
                os.remove(caminfoOUT)

            elif status == 'error':
                label = pvl.load(infile)
                date = datetime.datetime.now(pytz.utc).strftime(
                    "%Y-%m-%d %H:%M:%S")

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

                        """
                        target_Qobj = session.query(upc_models.Targets).filter(
                            upc_models.Targets.targetname == keywordsOBJ.getKeyword(
                                'Instrument', 'TargetName').upper()).first()

                        instrument_Qobj = session.query(upc_models.Instruments).filter(
                            upc_models.Instruments.instrument == keywordsOBJ.getKeyword(
                                'Instrument', 'InstrumentId')).first()

                        """
                        error1_input = upc_models.DataFiles(isisid='1',
                                                            edr_source=EDRsource)
                        session.add(error1_input)
                        session.commit()

                    EQ1obj = session.query(upc_models.DataFiles).filter(
                        upc_models.DataFiles.edr_source == EDRsource).first()
                    UPCid = EQ1obj.upcid

                    errorMSG = 'Error running {} on file {}'.format(
                        processError, inputfile)

                    # typeid 84 = processdate
                    DBinput = MetaTime(upcid=UPCid,
                                       typeid='84',  # 84 in prod
                                       value=date)
                    session.add(DBinput)

                    # typeid 673 = errortype
                    DBinput = MetaString(upcid=UPCid,
                                         typeid='673',  # 673 in prod
                                         value=processError)
                    session.add(DBinput)

                    # typeid 596 = errormessage
                    DBinput = MetaString(upcid=UPCid,
                                         typeid='596',  # 596 in prod
                                         value=errorMSG)
                    session.add(DBinput)

                    # typeid 595 = error
                    DBinput = MetaBoolean(upcid=UPCid,
                                          typeid='595',  # 595 in prod
                                          value='true')
                    session.add(DBinput)

                    # typeid 597 = isisfootprint
                    DBinput = MetaGeometry(upcid=UPCid,
                                           typeid='597',  # 597 in prod
                                           value='POINT(361 0)')
                    session.add(DBinput)

                    # typeid 598 = isiscentroid
                    DBinput = MetaGeometry(upcid=UPCid,
                                           typeid='598',  # 598 in prod
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
                            instruments.instrument == str(
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

                    # typeid 84 = processdate
                    DBinput = MetaTime(upcid=UPCid,
                                       typeid='84',  # 84 in prod
                                       value=date)
                    session.add(DBinput)

                    # typeid 673 = errortype
                    DBinput = MetaString(upcid=UPCid,
                                         typeid='673',  # 673 in prod
                                         value=processError)
                    session.add(DBinput)

                    # typeid 596 = errormessage
                    DBinput = MetaString(upcid=UPCid,
                                         typeid='596',  # 596 in prod
                                         value=errorMSG)
                    session.add(DBinput)

                    # typeid 595 = error flag
                    DBinput = MetaBoolean(upcid=UPCid,
                                          typeid='595',  # 595 in prod
                                          value='true')
                    session.add(DBinput)

                    # typeid 597 = isisfootprint
                    DBinput = MetaGeometry(upcid=UPCid,
                                           typeid='597',  # 597 in prod
                                           value='POINT(361 0)')
                    session.add(DBinput)

                    # typeid 598 = isiscentroid
                    DBinput = MetaGeometry(upcid=UPCid,
                                           typeid='598',  # 598 in prod
                                           value='POINT(361 0)')
                    session.add(DBinput)

                    # typeid 52 = starttime
                    DBinput = MetaTime(upcid=UPCid,
                                       typeid='52',  # 52 in prod
                                       value=label['IsisCube']['Instrument']['StartTime'])
                    session.add(DBinput)

                    # typeid 47 = stoptime
                    DBinput = MetaTime(upcid=UPCid,
                                       typeid='47',  # 47 in prod
                                       value=label['IsisCube']['Instrument']['StopTime'])
                    session.add(DBinput)

                    session.commit()

                AddProcessDB(pds_session, inputfile, 'f')
                os.remove(infile)


if __name__ == "__main__":
    sys.exit(main())
