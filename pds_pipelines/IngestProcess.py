#!/usgs/apps/anaconda/bin/python
import os
import sys
import datetime
import pytz
import logging
import json


import hashlib
import shutil

import sqlalchemy
from sqlalchemy import *
from sqlalchemy.orm.util import *

from pds_pipelines.RedisQueue import *
from pds_pipelines.PDS_DBquery import *
from pds_pipelines.db import db_connect
from pds_pipelines.config import pds_info
from pds_pipelines.models.pds_models import Files, Archives

import pdb



# @TODO there HAS to be a better way of doing this...
def getArchiveID(inputfile):
    """
    Parameters
    ----------
    inputfile : str

    Returns
    -------
    str
        archive
    """
    if 'Mars_Reconnaissance_Orbiter/CTX' in inputfile:
        archive = 'mroCTX'
    elif 'Mars_Reconnaissance_Orbiter/MARCI' in inputfile:
        archive = 'mroMARCI'
    elif 'Cassini/RADAR' in inputfile:
        archive = 'cassiniRADAR'
    elif 'Cassini/ISS' in inputfile:
        archive = 'cassiniISS'
    elif 'Cassini/VIMS' in inputfile:
        archive = 'cassiniVIMS'
    elif 'Dawn/Vesta' in inputfile:
        archive = 'dawnVesta'
    elif 'Dawn/Ceres' in inputfile:
        archive = 'dawnCeres'
    elif 'Galileo/SSI' in inputfile:
        archive= 'galileoSSI'
    elif 'MESSENGER' in inputfile:
        archive = 'messenger'
    elif 'Magellan' in inputfile:
        archive = 'magellan'
    elif 'THEMIS/USA_NASA_PDS_ODTSDP_100XX' in inputfile:
        if 'odtvb1' in inputfile or 'odtve1' in inputfile or 'odtvr1' in inputfile:
            archive = 'themisVIS_EDR'
        else:
            archive = 'themisIR_EDR'
    elif 'USA_NASA_PDS_ODTGEO_200XX' in inputfile:
        archive = 'themisGEO'
    elif 'Lunar_Reconnaissance_Orbiter/LROC/EDR' in inputfile:
        archive = 'lrolrcEDR'
    elif 'Lunar_Reconnaissance_Orbiter/LAMP' in inputfile:
        archive = 'lroLAMP'
    elif 'Lunar_Orbiter' in inputfile:
        archive = 'lunarOrbiter'
    elif 'Mars_Reconnaissance_Orbiter/HiRISE' in inputfile:
        if 'HiRISE/EDR/' in inputfile:
            archive = 'mroHIRISE_EDR'
        elif 'HiRISE/RDR/' in inputfile:
            archive = 'mroHIRISE_RDR'
        elif 'HiRISE/EXTRAS/' in inputfile:
            archive = 'mroHIRISE_EXTRAS'
        else:
            archive = 'mroHIRISE'
    elif 'Apollo/Rock_Sample_Images' in inputfile:
        archive = 'apolloRock'

    return archive


def main():

    #    pdb.set_trace()

    arch = sys.argv[-1]

# ********* Set up logging *************
    logger = logging.getLogger('Ingest_Process')
    logger.setLevel(logging.INFO)
    #logFileHandle = logging.FileHandler('/usgs/cdev/PDS/logs/Ingest.log')
    logFileHandle = logging.FileHandler('/home/arsanders/PDS-Pipelines/logs/Ingest.log')
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s, %(message)s')
    logFileHandle.setFormatter(formatter)
    logger.addHandler(logFileHandle)

    logger.info("Starting Process")
    PDSinfoDICT = json.load(open(pds_info, 'r'))

    RQ_main = RedisQueue('Ingest_ReadyQueue')
    RQ_work = RedisQueue('Ingest_WorkQueue')

    RQ_upc = RedisQueue('UPC_ReadyQueue')
    RQ_thumb = RedisQueue('Thumbnail_ReadyQueue')
    RQ_browse = RedisQueue('Browse_ReadyQueue')
    RQ_pilotB = RedisQueue('PilotB_ReadyQueue')

    try:
        session, _ = db_connect('pdsdi_dev')
        logger.info('DataBase Connecton: Success')
    except:
        logger.error('DataBase Connection: Error')

    index = 1

    while int(RQ_main.QueueSize()) > 0:

        inputfile = (RQ_main.Qfile2Qwork(
            RQ_main.getQueueName(), RQ_work.getQueueName())).decode('utf-8')
        archive = getArchiveID(inputfile)
        subfile = inputfile.replace(PDSinfoDICT[archive]['path'], '')

        # Gen a checksum from input file
        """
        CScmd = 'md5sum ' + inputfile
        process = subprocess.Popen(CScmd, stdout=subprocess.PIPE, shell=True)
        (stdout, stderr) = process.communicate()
        filechecksum = stdout.split()[0]
        """

        # Calculate checksum in chunks of 4096
        f_hash = hashlib.md5()
        with open(inputfile, "rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                f_hash.update(chunk)
        filechecksum = f_hash.hexdigest()

        QOBJ = session.query(Files).filter_by(filename=subfile).first()

        runflag = False
        if QOBJ is None:
            runflag = True
        elif filechecksum != QOBJ.checksum:
            runflag = True

        if runflag == True:
            date = datetime.datetime.now(
                pytz.utc).strftime("%Y-%m-%d %H:%M:%S")
            fileURL = inputfile.replace(
                '/pds_san/PDS_Archive/', 'pdsimage.wr.usgs.gov/Missions/')
            upcflag = False
            if int(PDSinfoDICT[archive]['archiveid']) == 124:
                if '.IMG' in inputfile:
                    upcflag = True
            elif int(PDSinfoDICT[archive]['archiveid']) == 74:
                if '/DATA/' in inputfile and '/NAC/' in inputfile and '.IMG' in inputfile:
                    upcflag = True
            elif int(PDSinfoDICT[archive]['archiveid']) == 16:
                if '/data/' in inputfile and '.IMG' in inputfile:
                    upcflag = True
            elif int(PDSinfoDICT[archive]['archiveid']) == 53:
                if '/data/' in inputfile and '.LBL' in inputfile:
                    upcflag = True
            elif int(PDSinfoDICT[archive]['archiveid']) == 116:
                if '/data/odtie1' in inputfile and '.QUB' in inputfile:
                    upcflag = True
            elif int(PDSinfoDICT[archive]['archiveid']) == 117:
                if '/data/odtve1' in inputfile and '.QUB' in inputfile:
                    upcflag = True

            if upcflag == True:
                RQ_upc.QueueAdd(inputfile)
                RQ_thumb.QueueAdd(inputfile)
                RQ_browse.QueueAdd(inputfile)
                RQ_pilotB.QueueAdd(inputfile)

            filesize = os.path.getsize(inputfile)

            try:
                testIN = Files()
                testIN.archiveid = PDSinfoDICT[archive]['archiveid']
                testIN.filename = subfile
                testIN.entry_date = date
                testIN.checksum = filechecksum
                testIN.upc_required = upcflag
                testIN.validation_required = True
                testIN.header_only = False
                testIN.release_date = date
                testIN.file_url = fileURL
                testIN.file_size = filesize
                testIN.di_pass = True
                testIN.di_date = date

                session.add(testIN)
                session.flush()

                RQ_work.QueueRemove(inputfile)

                index = index + 1

            except:
                logger.error("Error During File Insert %s", subfile)

        elif runflag == False:
            RQ_work.QueueRemove(inputfile)

        if index >= 250:
            try:
                session.commit()
#                logger.info("Commit 250 files to Database: Success")
                index = 1
            except:
                session.rollback()
                logger.error("Something Went Wrong During DB Insert")
    else:
        logger.info("No Files Found in Inget Queue")
        try:
            session.commit()
            logger.info("Commit to Database: Success")
        except:
            session.rollback()

    if RQ_main.QueueSize() == 0 and RQ_work.QueueSize() == 0:
        logger.info("Process Complete All Queues Empty")
    elif RQ_main.QueueSize() == 0 and RQ_work.QueueSize() != 0:
        logger.warning("Process Done Work Queue NOT Empty Contains %s Files", str(
            RQ_work.QueueSize()))

    logger.info("Ingest Complete")


if __name__ == "__main__":
    sys.exit(main())
