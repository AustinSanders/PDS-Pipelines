#!/usgs/apps/anaconda/bin/python
import os, sys, pvl, json, datetime, pytz
import logging
from pysis import isis
from pysis.exceptions import ProcessError

from pysis.isis import getsn

from RedisQueue import *
from Recipe import *

import sqlalchemy
from sqlalchemy import *
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm import mapper
from sqlalchemy import create_engine
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm.util import *
from sqlalchemy.ext.declarative import declarative_base

import pdb

def getArchive(inputfile):
    if 'Mars_Reconnaissance_Orbiter/CTX' in inputfile:
        archive = 'mroCTX'
    elif 'THEMIS/USA_NASA_PDS_ODTSDP_100XX' in inputfile and 'odtie1' in inputfile:
        archive = 'themisIR_EDR'
    elif 'THEMIS/USA_NASA_PDS_ODTSDP_100XX' in inputfile and 'odtve1' in inputfile:
        archive = 'themisVIS_EDR'
    elif 'Lunar_Reconnaissance_Orbiter/LROC/EDR' in inputfile:
        archive = 'lrolrcEDR'

    return archive
    
def getISISid(infile):

    SerialNum = getsn(from_=infile)
    newisisSerial = SerialNum.replace('\n', '')
    return newisisSerial


def scaleFactor(line, sample, jsonfile):

#    pdb.set_trace()

    infoDICT = json.load(open(jsonfile, 'r'))

    maxLine = int(infoDICT['inputinfo']['maxline'])
    maxSample = int(infoDICT['inputinfo']['maxsample'])
    minLine = int(infoDICT['inputinfo']['minline'])
    minSample = int(infoDICT['inputinfo']['minsample'])

    if sample < line:
        scalefactor = line/maxLine
        testsamp = int(sample/scalefactor)

        if testsamp < minSample:
            scalefactor = sample/minSample

    else:
        scalefactor = sample/maxSample
        testline = int(line/scalefactor)
         
        if testline < minLine:
            scalefactor = line/minLine

    if scalefactor < 1:
        scalefactor = 1
    return scalefactor

def makedir(inputfile):

#    pdb.set_trace()

    temppath = os.path.dirname(inputfile).lower()
    finalpath = temppath.replace('/pds_san/pds_archive/', '/pds_san/PDS_Derived/UPC/images/')

    if not os.path.exists(finalpath):
        try:
            os.makedirs(finalpath, 0755)
        except OSError as e:
            if e.errno != errno.EEXIST:
                raise

    return finalpath

def DB_addURL(isisSerial, inputfile):

#     pdb.set_trace()

     Base = declarative_base()
#     engine = create_engine('postgresql://upcmgr:un1pl@c0@dino.wr.usgs.gov:3309/upc_dev')
     engine = create_engine('postgresql://upcmgr:un1pl@c0@dino.wr.usgs.gov:3309/upc_prd')
     metadata = MetaData(bind=engine)
     class datafiles(Base):
         __table__ = Table('datafiles', metadata, autoload=True)


     class meta_string(Base):
         __table__ = Table('meta_string', metadata, autoload=True)

     Session = sessionmaker(bind=engine)
     session = Session()

     newisisSerial = isisSerial.split(':')[0]
     likestr = '%' + newisisSerial + '%'
     Qobj = session.query(datafiles).filter(datafiles.isisid.like(likestr)).first()

     if str(Qobj.isisid) == str(isisSerial):

         outputfile = inputfile.replace('/pds_san/PDS_Derived/UPC/images/', '$thumbnail_server/')
 
         DBinput = meta_string(upcid=Qobj.upcid,
                                typeid='348',
                                value=outputfile)

         try:
             session.add(DBinput)
             session.commit()
             return 'SUCCESS'
         except:
             return 'ERROR'

def AddProcessDB(inputfile, outvalue):

#    pdb.set_trace()

    Base = declarative_base()
    engine = create_engine('postgresql://pdsdi:dataInt@dino.wr.usgs.gov:3309/pds_di_prd')
    metadata = MetaData(bind=engine)

    class files(Base):
        __table__ = Table('files', metadata, autoload=True)

    class process_runs(Base):
        __table__ = Table('process_runs', metadata, autoload=True)

    Session = sessionmaker(bind=engine)
    session =  Session()

    parts = inputfile.split("/")

    testfile = parts[-3] + "/" + parts[-2] + "/" + parts[-1]
    testfile2 = '%' + testfile + '%'

    fileQobj = session.query(files).filter(files.filename.like(testfile2)).first()
    fileid = fileQobj.fileid

    date = datetime.datetime.now(pytz.utc).strftime("%Y-%m-%d %H:%M:%S")



    processDB = process_runs(fileid=fileQobj.fileid,
                             process_date=date,
                             process_typeid='3',
                             process_out = outvalue)

    try:
        session.add(processDB)
        session.commit()
        return 'SUCCESS'
    except:
        return 'ERROR'


def main():

#    pdb.set_trace()

    workarea = '/scratch/pds_services/workarea/'

##***************** Set up logging *****************
    logger = logging.getLogger('Thumbnail_Process')
    logger.setLevel(logging.INFO)
    logFileHandle = logging.FileHandler('/usgs/cdev/PDS/logs/Process.log')
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s, %(message)s')
    logFileHandle.setFormatter(formatter)
    logger.addHandler(logFileHandle)    

    RQ_main = RedisQueue('Thumbnail_ReadyQueue')

    while int(RQ_main.QueueSize()) > 0:

        inputfile = RQ_main.QueueGet()
        if os.path.isfile(inputfile):
            logger.info('Starting Process: %s', inputfile)

#            if 'Mars_Reconnaissance_Orbiter/CTX/' in inputfile:
            archive = getArchive(inputfile)

## ********** Derived DIR path Stuff **********************

            finalpath = makedir(inputfile)                  


            recipeOBJ = Recipe()
            recip_json = recipeOBJ.getRecipeJSON(archive, 'thumbnail')
            recipeOBJ.AddJsonFile(recip_json)


            infile = workarea + os.path.splitext(os.path.basename(inputfile))[0] + '.Tinput.cub'
            outfile = workarea + os.path.splitext(os.path.basename(inputfile))[0] + '.Toutput.cub'
            
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
                        PDSinfoDICT = json.load(open('/usgs/cdev/PDS/bin/PDSinfo.json', 'r'))
                        exband = 'none'
                        label = pvl.load(infile)
                        for item1 in PDSinfoDICT[getArchive(inputfile)]['bandorder']:
                            for item2 in label['IsisCube']['BandBin'][PDSinfoDICT[getArchive(inputfile)]['bandbinQuery']]:
                                if str(item1) == str(item2):
                                    exband = item2
                                    break
                            if exband != 'none':
                                break
                        band_infile = infile + '+' + str(exband)
                        processOBJ.updateParameter('from_', band_infile)
                        processOBJ.updateParameter('to', outfile)

                    elif item == 'ctxevenodd':
                        label = pvl.load(infile)
                        SS = label['IsisCube']['Instrument']['SpatialSumming']
                        if SS != 1:
                            break
                        else:
                            processOBJ.updateParameter('from_', infile)
                            processOBJ.updateParameter('to', outfile)

                    elif item == 'reduce':
                        label = pvl.load(infile)
                        Nline = label['IsisCube']['Core']['Dimensions']['Lines']
                        Nsample = label['IsisCube']['Core']['Dimensions']['Samples']
                        Nline = int(Nline)
                        Nsample = int(Nsample)
                        Sfactor = scaleFactor(Nline, Nsample, recip_json)
                        processOBJ.updateParameter('lscale', Sfactor)
                        processOBJ.updateParameter('sscale', Sfactor)
                        processOBJ.updateParameter('from_', infile)
                        processOBJ.updateParameter('to', outfile)
                    elif item == 'isis2std':
                        final_outfile = finalpath + '/' + os.path.splitext(os.path.basename(inputfile))[0] + '.thumbnail.jpg'
                        processOBJ.updateParameter('from_', infile)
                        processOBJ.updateParameter('to', final_outfile)

                    else:
                        processOBJ.updateParameter('from_', infile)
                        processOBJ.updateParameter('to', outfile)

                    print processOBJ.getProcess()

                    for k, v in processOBJ.getProcess().items():
                        func = getattr(isis, k)
                        try:
                            func(**v)
                            logger.info('Process %s :: Success', k)
                            if os.path.isfile(outfile):
                                if '.cub' in outfile:
                                    os.rename(outfile, infile)
                            status = 'success'
                            if '2isis' in item:
                                isisSerial = getISISid(infile)
                        except ProcessError as e:
                            logger.error('Process %s :: Error', k)
                            status = 'error'

      
            if status == 'success':
                testout = DB_addURL(isisSerial, final_outfile)
                os.remove(infile)
                logger.info('Thumbnail Process Success: %s', inputfile)

                AddProcessDB(inputfile, 't')  
        else:
            logger.error('File %s Not Found', inputfile)

if __name__ == "__main__":
    sys.exit(main())
