#!/usgs/apps/anaconda/bin/python
import os, sys, pvl
import logging
import json

from pysis import isis
from pysis.exceptions import ProcessError

from RedisQueue import *
from Recipe import *
from UPCkeywords import *
from UPC_test import *


from sqlalchemy import *
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm import mapper
from sqlalchemy import create_engine
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm.util import *
from sqlalchemy.ext.declarative import declarative_base
from geoalchemy2 import Geometry
#from geoalchemy2.shape import to_shape


import pdb

def getMission(inputfile):

    if 'Mars_Reconnaissance_Orbiter/CTX' in inputfile:
        mission = 'mroCTX'
    elif 'USA_NASA_PDS_ODTSDP_100XX' in inputfile:
        mission = 'themisIR_EDR'
    elif 'Lunar_Reconnaissance_Orbiter/LROC/EDR/' in inputfile:
        mission = 'lrolrcEDR'

    return mission

def find_keyword(obj, key):
    if key in obj:
        return obj[key]
    for k, v in obj.items():
        if isinstance(v,dict):
            F_item = find_keyword(v, key)
            if F_item is not None:
                return F_item

def addKeyword(session, type, Iupcid, Itypeid, Ivalue):

    if type == 'string':
         DBin = meta_string(upcid=Iupcid, typeid=Itypeid, value=Ivalue)
    elif type == 'double':
         DBin = meta_precision(upcid=Iupcid, typeid=Itypeid, value=Ivalue)
    elif type == 'time':
         DBin = meta_time(upcid=Iupcid, typeid=Itypeid, value=Ivalue)
    elif type == 'integer':
         DBin = meta_integer(upcid=Iupcid, typeid=Itypeid, value=Ivalue)
    elif type == 'boolean':
         DBin = meta_boolean(upcid=Iupcid, typeid=Itypeid, value=Ivalue)

    session.add(DBin)
    session.commit()

def main():

    pdb.set_trace()

    workarea = '/scratch/pds_services/workarea/'

    Base = declarative_base()

    engine = create_engine('postgresql://{}:{}@{}:{}/{}'.format(upcdev_user,
                                                                upcdev_pass,
                                                                upcdev_host,
                                                                upcdev_port,
                                                                upcdev_db))


    metadata = MetaData(bind=engine)

    class datafiles(Base):
        __table__ = Table('datafiles', metadata, autoload=True)

    class instruments(Base):
        __table__ = Table('instruments_meta', metadata, autoload=True)

    class targets(Base):
        __table__ = Table('targets_meta', metadata, autoload=True)

    class keywords(Base):
        __table__ = Table('keywords', metadata, autoload=True)

    class meta_precision(Base):
        __table__ = Table('meta_precision', metadata, autoload=True)
 
    class meta_time(Base):
        __table__ = Table('meta_time', metadata, autoload=True)

    class meta_string(Base):
        __table__ = Table('meta_string', metadata, autoload=True)

    class meta_integer(Base):
        __table__ = Table('meta_integer', metadata, autoload=True)

    class meta_boolean(Base):
        __table__ = Table('meta_boolean', metadata, autoload=True)

    class meta_geometry(Base):
        __tablename__ = 'meta_geometry'
        upcid = Column(Integer, primary_key=True)
        typeid = Column(Integer)
        value = Column(Geometry('geometry'))


    Session = sessionmaker(bind=engine)
    session = Session()

##***************** Set up logging *****************
    logger = logging.getLogger('UPC_Process')
    logger.setLevel(logging.INFO)
    logFileHandle = logging.FileHandler('/usgs/cdev/PDS/logs/Process.log')
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s, %(message)s')
    logFileHandle.setFormatter(formatter)
    logger.addHandler(logFileHandle)

    PDSinfoDICT = json.load(open('/usgs/cdev/PDS/bin/PDSinfo.json', 'r'))

    RQ_main = RedisQueue('UPC_ReadyQueue') 

    while int(RQ_main.QueueSize()) > 0:

        inputfile = RQ_main.QueueGet()
        if os.path.isfile(inputfile):
            logger.info('Starting Process: %s', inputfile)

            recipeOBJ = Recipe()
            recip_json = recipeOBJ.getRecipeJSON(getMission(inputfile), 'upc')
            recipeOBJ.AddJsonFile(recip_json)

            infile = workarea + os.path.splitext(os.path.basename(inputfile))[0] + '.UPCinput.cub'
            outfile = workarea + os.path.splitext(os.path.basename(inputfile))[0] + '.UPCoutput.cub'  
            caminfoOUT = workarea + os.path.splitext(os.path.basename(inputfile))[0] + '_caninfo.pvl'
            EDRsource = inputfile.replace('/pds_san/PDS_Archive/', 'https://pdsimage.wr.ugs.gov/Missions/')

            status = 'success'
            for item in recipeOBJ.getProcesses():
                if status == 'error':
                    break
                elif status == 'success':
                    processOBJ = Process()
                    processR = processOBJ.ProcessFromRecipe(item, recipeOBJ.getRecipe())

                    if '2isis' in item:
                        processOBJ.updateParameter('from_', inputfile) 
                        processOBJ.updateParameter('to', outfile)
                    elif item == 'spiceinit':
                        processOBJ.updateParameter('from_', infile)
                    elif item == 'cubeatt':
                        exband = 'none'
                        label = pvl.load(infile)
                        for item1 in PDSinfoDICT[getMission(infile)]['bandorder']:
                            print 'test of PB: %s' % item1
                            for item2 in label['IsisCube']['BandBin'][PDSinfoDICT[getMission(inputfile)]['bandbinQuery']]:
                                print 'test of labels: %s' % item2
                                if str(item1) == str(item2):
                                    print 'we have a match'
                                    exband = item2
                                    break
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

#                   test of process
                    print processOBJ.getProcess() 

                    for k, v in processOBJ.getProcess().items():
                        func = getattr(isis, k)
                        try:
                            func(**v)
                            if os.path.isfile(outfile):
                               os.rename(outfile, infile)
                            status = 'success'
                        except ProcessError as e:
                            status = 'error'
## keyword definition stuff ##
#            label = pvl.load(caminfoOUT)
            keywordsOBJ =  UPCkeywords(caminfoOUT)
            PVL_isisid = keywordsOBJ.getKeyword('Parameters', 'IsisId')

            print 'Test of isisid: %s' % PVL_isisid

            test_QD = session.query(datafiles).filter(datafiles.isisid == PVL_isisid).first()

            target_Qobj = session.query(targets).filter(targets.targetname == keywordsOBJ.getKeyword('Instrument', 'TargetName').upper()).first()
            print 'test of target ID: %s' % target_Qobj.targetid

            if session.query(datafiles).filter(datafiles.isisid == keywordsOBJ.getKeyword('Parameters', 'IsisId')).first() == None:

                target_Qobj = session.query(targets).filter(targets.targetname == keywordsOBJ.getKeyword('Instrument', 'TargetName').upper()).first()
                instrument_Qobj = session.query(instruments).filter(instruments.instrument == keywordsOBJ.getKeyword('Instrument', 'InstrumentId')).first()

                PVL_productid = keywordsOBJ.getKeyword('Archive', 'ProductId')
                print 'Test of productid: %s' % PVL_productid
                test_input = datafiles(isisid=keywordsOBJ.getKeyword('Parameters', 'IsisId'),
                                       productid=keywordsOBJ.getKeyword('Archive', 'ProductId'),
                                       edr_source=EDRsource,
                                       instrumentid=instrument_Qobj.instrumentid,
                                       targetid=target_Qobj.targetid)
                session.add(test_input)
                session.commit()

            Qobj = session.query(datafiles).filter(datafiles.isisid == PVL_isisid).first()
            UPCid = Qobj.upcid

            testjson = json.load(open('/usgs/cdev/PDS/recipe/test_Keyword_Definition.json', 'r')) 
            for element_1 in testjson['instrument']['COMMON']:
                print 'Inside element_1 test: %s' % element_1
                keytype = testjson['instrument']['COMMON'][element_1]['type']
                keygroup = testjson['instrument']['COMMON'][element_1]['group']
                keyword = testjson['instrument']['COMMON'][element_1]['keyword']

                keyword_Qobj = session.query(keywords).filter(keywords.typename == element_1).first()
                print 'test of keyword typeid: %s' % str(keyword_Qobj.typeid)

                if keygroup == 'Polygon':
                    print 'Polygon Keyword'
                    keyvalue = keywordsOBJ.getPolygonKeyword(keyword)
                else:
                    keyvalue = keywordsOBJ.getKeyword(keygroup, keyword)
                print 'COMMON keyvalue is: %s' % keyvalue

                if keytype == 'string':
                    DBinput = meta_string(upcid=UPCid,
                                             typeid=keyword_Qobj.typeid,
                                             value=keyvalue)
                elif keytype == 'double':
                    DBinput = meta_precision(upcid=UPCid,
                                             typeid=keyword_Qobj.typeid,
                                             value=keyvalue)
                elif keytype == 'integer':
                    DBinput = meta_integer(upcid=UPCid,
                                             typeid=keyword_Qobj.typeid,
                                             value=keyvalue)
                elif keytype == 'time':
                    DBinput = meta_time(upcid=UPCid,
                                             typeid=keyword_Qobj.typeid,
                                             value=keyvalue)

                elif keytype == 'boolean':
                    DBinput = meta_boolean(upcid=UPCid,
                                             typeid=keyword_Qobj.typeid,
                                             value=keyvalue)


                session.add(DBinput)
            session.commit()

# geometry stuff
            G_centroid = 'point (' + str(keywordsOBJ.getKeyword('Polygon', 'CentroidLongitude')) + ' ' + str(keywordsOBJ.getKeyword('Polygon', 'CentroidLatitude')) + ')'
            G_keyword_Qobj = session.query(keywords).filter(keywords.typename == 'isiscentroid').first()
            G_DBinput = meta_geometry(upcid=UPCid,
                                      typeid=G_keyword_Qobj.typeid,
                                      value=G_centroid)
            session.add(G_DBinput)
        
            G_footprint = keywordsOBJ.getKeyword('Polygon', 'GisFootprint')
            G_footprint_Qobj = session.query(keywords).filter(keywords.typename == 'isisfootprint').first()
            G_DBinput = meta_geometry(upcid=UPCid,
                                      typeid=G_footprint_Qobj.typeid,
                                      value=G_footprint)
            session.add(G_DBinput)
            session.commit()            

            for element_2 in testjson['instrument'][getMission(inputfile)]:
                print 'Inside element_2 test: %s' % element_2
                M_keytype = testjson['instrument'][getMission(inputfile)][element_2]['type']
                M_keygroup = testjson['instrument'][getMission(inputfile)][element_2]['group']
                M_keyword = testjson['instrument'][getMission(inputfile)][element_2]['keyword']

                M_keyword_Qobj = session.query(keywords).filter(keywords.typename == element_2).first()

                if M_keytype == 'string':
                    DBinput = meta_string(upcid=UPCid,
                                             typeid=M_keyword_Qobj.typeid,
                                             value=keywordsOBJ.getKeyword(M_keygroup, M_keyword))
                elif M_keytype == 'double':
                    DBinput = meta_precision(upcid=UPCid,
                                             typeid=M_keyword_Qobj.typeid,
                                             value=keywordsOBJ.getKeyword(M_keygroup, M_keyword))
                elif M_keytype == 'integer':
                    DBinput = meta_integer(upcid=UPCid,
                                             typeid=M_keyword_Qobj.typeid,
                                             value=keywordsOBJ.getKeyword(M_keygroup, M_keyword))
                elif M_keytype == 'time':
                    DBinput = meta_time(upcid=UPCid,
                                             typeid=M_keyword_Qobj.typeid,
                                             value=keywordsOBJ.getKeyword(M_keygroup, M_keyword))

                elif M_keytype == 'boolean':
                    DBinput = meta_boolean(upcid=UPCid,
                                             typeid=M_keyword_Qobj.typeid,
                                             value=keywordsOBJ.getKeyword(M_keygroup, M_keyword))

                session.add(DBinput)
            session.commit()

if __name__ == "__main__":
    sys.exit(main())
