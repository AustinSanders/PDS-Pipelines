#!/usr/bin/env python

import os
import sys
import subprocess
import logging
import shutil
import datetime
import json
import argparse
import xml.etree.ElementTree as ET

from io import BytesIO
from collections import OrderedDict
from pds_pipelines.redis_queue import RedisQueue
from pds_pipelines.redis_hash import RedisHash
from pds_pipelines.pds_db_query import PDS_DBquery
from pds_pipelines.config import pds_log, pow_map2_base, workarea, default_namespace
from pysis import ISIS_VERSION as isis_version



def renderError(errorhash):
    """
    Parameters
    ----------
    errorhash
    """
    for key in errorhash.getKeys():
        print(key)


def parse_args():
    parser = argparse.ArgumentParser(description="Service Final")

    parser.add_argument('--log', '-l', dest="log_level",
                        choices=['DEBUG', 'INFO',
                                'WARNING', 'ERROR', 'CRITICAL'],
                        help="Set the log level.", default='INFO')

    parser.add_argument('--namespace',
                        '-n',
                        dest='namespace',
                        help="Queue namespace")

    parser.add_argument('--key',
                        '-k',
                        dest='key',
                        help='Job key')

    args = parser.parse_args()
    return args


def main(user_args):
    key = user_args.key
    namespace = user_args.namespace

    if namespace is None:
        namespace = default_namespace

    log_level = user_args.log_level

#***************** Setup Logging **************
    logger = logging.getLogger('service_final' + key)
    level = logging.getLevelName(log_level)
    logger.setLevel(level)
    logFileHandle = logging.FileHandler(pds_log+'Service.log')

    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s, %(message)s')
    logFileHandle.setFormatter(formatter)
    logger.addHandler(logFileHandle)

    logger.info('Starting Final Process')
#************Set up REDIS Queues ****************
    zipQueue = RedisQueue(key + '_ZIP', namespace)
    loggyQueue = RedisQueue(key + '_loggy', namespace)
    infoHash = RedisHash(key + '_info')
    recipeQueue = RedisQueue(key + '_recipe', namespace)
    errorHash = RedisHash(key + '_error')

    DBQO = PDS_DBquery('JOBS')

    if errorHash.HashCount() > 0:
        root = ET.Element('errors')

        test = errorHash.getKeys()
        for item in test:
            sub = ET.Element('error')
            root.append(sub)

            field1 = ET.SubElement(sub, 'file')
            if isinstance(item, bytes):
                item = item.decode('utf-8')
            field1.text = item

            Eval = errorHash.getError(item)
            if isinstance(Eval, bytes):
                Eval = Eval.decode('utf-8')

            field2 = ET.SubElement(sub, 'message')
            field2.text = Eval

        tree = ET.ElementTree(root)

#        testfile = 'test.xml'
#        with open(testfile, "w") as fh:

        fh = BytesIO()
        tree.write(fh, encoding='utf-8', xml_declaration=True)
        errorxml = str(fh.getvalue(), 'utf-8').replace("\n","")
        testval = DBQO.addErrors(key , errorxml)
        if testval == 'Success':
            logger.info('Error XML added to JOBS DB')
        elif testval == 'Error':
            logger.error('Adding Error XML to JOBS DB: Error')
        print(errorxml)

    Fdir = os.path.join(pow_map2_base, infoHash.Service(), key)
    Wpath = os.path.join(workarea, key)

    # Make final directory
    if not os.path.exists(Fdir):
        try:
            os.makedirs(Fdir)
            logger.info('Final Location Success: %s', Fdir)
        except:
            logger.error('Error Making Final Directory')

    # Block to build job log file

    outputLOG = Wpath + "/" + key + '.log'
    logOBJ = open(outputLOG, "w")

    logOBJ.write("       U.S. Geological Survey Cloud Processing Services\n")
    logOBJ.write("                http://astrocloud.wr.usgs.gov\n\n")

    if infoHash.Service() == 'POW':
        logOBJ.write("                 Processing On the Web(POW)\n\n")

    logOBJ.write("       Processing Provided by ASTROGEOLOGY USGS Flagstaff\n")
    logOBJ.write("              Contact Information: astroweb@usgs.gov\n\n")
    logOBJ.write(
        "____________________________________________________________________\n\n")

    logOBJ.write("JOB INFORMATION\n\n")
    logOBJ.write("     SERVICE:         " + infoHash.Service() + "\n")
    logOBJ.write("     JOB KEY:         " + key + "\n")
    logOBJ.write("     PROCESSING DATE: " +
                 datetime.datetime.now().strftime("%Y-%m-%d %H:%M") + "\n")

    logOBJ.write("     ISIS VERSION:   " + isis_version)

    if infoHash.getStatus() == 'ERROR':
        logOBJ.write("     JOB STATUS:      " +
                     infoHash.getStatus() + " See Details Below\n")
    else:
        logOBJ.write("     JOB STATUS:      " + infoHash.getStatus() + "\n")
    logOBJ.write("     FILE COUNT:      " + infoHash.getFileCount() + "\n\n")
    logOBJ.write(
        "_____________________________________________________________________\n\n")

    logOBJ.write("PROCESSING INFORMATION\n\n")
    for element in loggyQueue.ListGet():
        procDICT = json.loads(element, object_pairs_hook=OrderedDict)
        for infile in procDICT:
            logOBJ.write("     IMAGE: " + infile + "\n")
            for proc, _ in procDICT[infile].items():
                logOBJ.write("          PROCESS:  " + str(proc) + "\n")
                for k, val in procDICT[infile][proc].items():
                    if k == 'status':
                        logOBJ.write(
                            "               STATUS:     " + val + "\n")
                    elif k == 'parameters':
                        logOBJ.write(
                            "               PARAMETERS: " + val + "\n")
                    elif k == 'helplink':
                        logOBJ.write(
                            "               HELP LINK:  " + val + "\n\n")
                    elif k == 'error':
                        logOBJ.write(
                            "               ERROR:      " + val + "\n\n")

    logOBJ.write("END-PROCESSING\n")
    logOBJ.close()

#******** Block for to copy and zip files to final directory ******
    map_file = os.path.join(Wpath, key + '.map')
    Zfile = os.path.join(Wpath, key + '.zip')

    final_file_list = zipQueue.ListGet()
    final_file_list.append(outputLOG)
    final_file_list.append(map_file)

    logger.info('Making Zip File %s', Zfile)
    with zipfile.ZipFile(Zfile, 'w') as out_zip:
        for item in final_file_list:
            try:
                logger.info('File %s added to zip file: success', item)
                out_zip.write(item,os.path.basename(item))
            except:
                logger.error('File %s NOT added to zip file', item)

    logger.info('Copying output files to %s', Fdir)
    for item in final_file_list:
        try:
            shutil.copyfile(item,os.path.join(Fdir, os.path.basename(item)))
            logger.info('Copied file %s to download directory: Success', item)
        except:
            logger.error('File %s NOT COPIED to download directory', item)
            logger.error(e)

    try:
        shutil.copy(Zfile,os.path.join(Fdir, os.path.basename(Zfile)))
        logger.info('Copied zip file %s to download directory', Zfile)
    except IOError as e:
        logger.error('Error while attempting to copy zip file %s to download area', Zfile)
        logger.error(e)

#************** Clean up *******************
    try:
        shutil.rmtree(Wpath)
        logger.info('Working Directory Removed: Success')
    except:
        logger.error('Working Directory NOT Removed')

    DBQO2 = PDS_DBquery('JOBS')
    DBQO2.setJobsFinished(key )

    errorHash.RemoveAll()
    infoHash.RemoveAll()
    loggyQueue.RemoveAll()
    zipQueue.RemoveAll()
    recipeQueue.RemoveAll()


    logger.info('Job %s is Complete', key)


if __name__ == "__main__":
    sys.exit(main(parse_args()))
