#!/usr/bin/env python

import os
import sys
import pvl
import lxml.etree as ET
import logging
import argparse
import json

from pds_pipelines.pds_db_query import PDS_DBquery
from pds_pipelines.redis_queue import RedisQueue
from pds_pipelines.redis_hash import RedisHash
from pds_pipelines.recipe import Recipe
from pds_pipelines.process import Process
from pds_pipelines.make_map import MakeMap
from pds_pipelines.hpc_job import HPCjob
from pds_pipelines.redis_lock import RedisLock
from pds_pipelines.config import recipe_base, pds_log, archive_base, default_namespace, slurm_log, cmd_dir, pds_info, lock_obj, workarea


class jobXML(object):

    def __init__(self, xml):
        """
        Parameters
        ----------
        xml
        """
        self.pds_info = json.load(open(pds_info, 'r'))
        self.root = ET.fromstring(xml.encode())

    def getInst(self):
        """
        Returns
        -------
        str
            inst
        """
        for info in self.root.findall('.//Process'):
            inst = info.find('.//instrument').text
            return inst

    def getCleanName(self):
        """ Get the internally consistent representation of the instrument name.

        Searches the PDSinfo dict for the 'clean name' that matches the recipes.  This function essentially
        maps URL->file path->internally consistent name.
        """
        # @TODO Fix after refactor
        # NOTE: I know this is really, really bad.  We're kind of backed into a corner here,
        #  and partial string matching on a value-based lookup of a nested dict is the temporary solution.

        # Get any file listed.  Assumes that all files are from the same instrument
        file_name = self.getFileList()[0]
        file_name = file_name.replace('http://pdsimage.wr.usgs.gov/Missions/', archive_base)

        candidates = []

        for key in self.pds_info:
            if file_name.startswith(self.pds_info[key]['path']):
                candidates.append(key)

        # try to filter list based on upc_reqs.  If upc_reqs isn't specified, just skip the filtering step
        # ps can't use 'filter' because not all elements have upc_reqs, so they may raise exceptions.
        if len(candidates) > 1:
            for item in candidates:
                try:
                    if not all(x in file_name for x in self.pds_info[item]['upc_reqs']):
                        candidates.remove(item)
                except KeyError:
                    # Intentionally left blank.  Unspecified upc_reqs is valid -- there's just nothing to do for those elements
                    pass

        # If multiple candidates still exist, then it is not possible to uniquely identify the clean name
        if len(candidates) > 1:
            raise(RuntimeError('Multiple candidates found for {} with no resolvable clean name'.format(file_name)))

        try:
            return candidates[0]
        except IndexError:
            raise(KeyError('No key found in PDSInfo dict for path {}'.format(file_name)))



    def getProcess(self):
        """
        Returns
        -------
        str
            PT
        """
        for info in self.root.findall('Process'):
            PT = info.find('ProcessName').text
            return PT

    def getTargetName(self):
        """
        Returns
        -------
        NoneType
            'None' if 'info.find('.//TargetName')' is None
        str
            otherwise return 'info.find('.//TargetName').text'
        """
        for info in self.root.iter('Target'):
            if info.find('.//TargetName') is None:
                return None
            else:
                return info.find('.//TargetName').text

    def getERadius(self):
        """
        Returns
        -------
        NoneType
            'None' if 'info.find('.//EquatorialRadius')' is None
        str
            Otherwise return 'info.find('.//EquatorialRadius').text'
        """
        for info in self.root.iter('Target'):
            if info.find('.//EquatorialRadius') is None:
                return None
            else:
                return info.find('.//EquatorialRadius').text

    def getPRadius(self):
        """
        Returns
        -------
        NoneType
            'None' if 'info.find('.//PolarRadius')' is None
        str
            Otherwise return 'info.find('.//PolarRadius').text'
        """
        for info in self.root.iter('Target'):
            if info.find('.//PolarRadius') is None:
                return None
            else:
                return info.find('.//PolarRadius').text

    def getLatType(self):
        """
        Returns
        -------
        NoneType
            'None' if 'info.find('.//LatitudeType')' is None
        str
            Otherwise return 'LT'
        """
        for info in self.root.iter('Target'):
            if info.find('.//LatitudeType') is None:
                return None
            else:
                temp_LT = info.find('.//LatitudeType').text
                if temp_LT == 'planetocentric':
                    LT = 'Planetocentric'
                elif temp_LT == 'planetographic':
                    LT = 'Planetographic'
            return LT

    def getLonDirection(self):
        """
        Returns
        -------
        NoneType
            'None' if 'info.find('.//LongitudeDirection')' is None
        str
            Otherwise return 'LD'
        """
        for info in self.root.iter('Target'):
            if info.find('.//LongitudeDirection') is None:
                return None
            else:
                temp_LD = info.find('.//LongitudeDirection').text
                if temp_LD == 'POSITIVEEAST':
                    LD = 'PositiveEast'
                elif temp_LD == 'POSITIVEWEST':
                    LD = 'PositiveWest'
            return LD

    def getLonDomain(self):
        """
        Returns
        -------
        NoneType
            'None' if 'info.find('.//LongitudeDomain')' is None
        str
            otherwise return 'info.find('.//LongitudeDomain').text'
        """
        for info in self.root.iter('Target'):
            if info.find('.//LongitudeDomain') is None:
                return None
            else:
                return info.find('.//LongitudeDomain').text

    def getProjection(self):
        """
        Returns
        -------
        NoneType
            'None' if 'proj' is None
        str
            Otherwise 'info.find('ProjName').text'
        """
        for info in self.root.iter('Projection'):
            proj = info.find('ProjName').text
            if proj is None:
                return None
            else:
                return info.find('ProjName').text

    def getClon(self):
        """
        Returns
        -------
        NoneType
            'None' if 'clon' is None
        str
            Otherwise 'info.find('CenterLongitude').text'
        """
        for info in self.root.iter('Projection'):
            clon = info.find('CenterLongitude')
            if clon is None:
                return None
            else:
                return info.find('CenterLongitude').text

    def getClat(self):
        """
        Returns
        -------
        NoneType
            'None' if 'clat' is None
        str
            Otherwise 'info.find('CenterLatitude').text'
        """
        for info in self.root.iter('Projection'):
            clat = info.find('CenterLatitude')
            if clat is None:
                return None
            else:
                return info.find('CenterLatitude').text

    def getFirstParallel(self):
        """
        Returns
        -------
        NoneType
            'None' if 'info.find('.//FirstStandardParallel')' is None
        str
            Otherwise 'info.find('.//FirstStandardParallel').text'
        """
        for info in self.root.iter('Projection'):
            if info.find('.//FirstStandardParallel') is None:
                return None
            else:
                return info.find('.//FirstStandardParallel').text

    def getSecondParallel(self):
        """
        Returns
        -------
        NoneType
            'None' if 'info.find('.//SecondStandardParallel')' is None
        str
            Otherwise 'info.find('.//SecondStandardParallel').text'
        """
        for info in self.root.iter('Projection'):
            if info.find('.//SecondStandardParallel') is None:
                return None
            else:
                return info.find('.//SecondStandardParallel').text

    def OutputGeometry(self):
        """
        Returns
        -------
        NoneType
            None if 'info' is None
        bool
            True if 'info' is not None
        """
        for info in self.root.iter('OutputGeometry'):
            if info is None:
                return None
            elif info is not None:
                return True

    def getRangeType(self):
        """
        Returns
        -------
        NoneType
            'None' if 'info.find('.//extentType')' is None
        str
            Otherwise 'info.find('.//extentType').text'
        """
        for info in self.root.iter('extents'):
            if info.find('.//extentType') is None:
                return None
            else:
                return info.find('.//extentType').text

    def getMinLat(self):
        """
        Returns
        -------
        NoneType
            'None' if 'info.find('.//MinLatitude')' is None
        str
            Otherwise 'info.find('.//MinLatitude').text'
        """
        for info in self.root.iter('extents'):
            if info.find('.//MinLatitude') is None:
                return None
            else:
                return info.find('.//MinLatitude').text

    def getMaxLat(self):
        """
        Returns
        -------
        NoneType
            'None' if 'info.find('.//MaxLatitude')' is None
        str
            Otherwise 'info.find('.//MaxLatitude').text'
        """
        for info in self.root.iter('extents'):
            if info.find('.//MaxLatitude') is None:
                return None
            else:
                return info.find('.//MaxLatitude').text

    def getMinLon(self):
        """
        Returns
        -------
        NoneType
            'None' if 'info.find('.//MinLongitude')' is None
        str
            Otherwise 'info.find('.//MinLongitude').text'
        """
        for info in self.root.iter('extents'):
            if info.find('.//MinLongitude') is None:
                return None
            else:
                return info.find('.//MinLongitude').text

    def getMaxLon(self):
        """
        Returns
        -------
        NoneType
            'None' if 'info.find('.//MaxLongitude')' is None
        str
            Otherwise 'info.find('.//MaxLongitude').text'
        """
        for info in self.root.iter('extents'):
            if info.find('.//MaxLongitude') is None:
                return None
            else:
                return info.find('.//MaxLongitude').text

    def getResolution(self):
        """
        Returns
        -------
        NoneType
            'None' if 'info.find('.//OutputResolution')' is None
        str
            Otherwise 'info.find('.//OutputResolution').text'
        """
        for info in self.root.iter('OutputOptions'):
            if info.find('.//OutputResolution') is None:
                return None
            else:
                return info.find('.//OutputResolution').text

    def getGridInterval(self):
        """
        Returns
        -------
        NoneType
            'None' if 'info.find('.//interval')' is None
        str
            Otherwise 'info.find('.//interval').text'
        """
        for info in self.root.iter('grid'):
            if info.find('.//interval') is None:
                return None
            else:
                return info.find('.//interval').text

    def getOutBit(self):
        """
        Returns
        -------
        str
            'input' if 'info.find('.//BitType')' is None,
            otherwise 'info.find('.//BitType').text'
        """
        for info in self.root.findall('.//OutputType'):
            if info.find('.//BitType') is None:
                return 'input'
            else:
                return info.find('.//BitType').text

    def getOutFormat(self):
        """
        Returns
        -------
        str
            outputFormat
        """
        for info in self.root.findall('.//OutputType'):
            outputFormat = info.find('.//Format').text
            return outputFormat

    def STR_Type(self):
        """
        Returns
        -------
        NoneType
            None
        str
            'StretchPercent', 'HistogramEqualization', 'GaussStretch',
            "SigmaStretch'
        """
        for info in self.root.findall('.//Process'):
            if info.find('.//stretch') is None:
                return None
            elif info.find('.//StretchPercent') is not None:
                return 'StretchPercent'
            elif info.find('.//HistogramEqualization') is not None:
                return 'HistogramEqualization'
            elif info.find('.//GaussStretch') is not None:
                return 'GaussStretch'
            elif info.find('.//SigmaStretch') is not None:
                return 'SigmaStretch'

    def STR_PercentMin(self):
        """
        Returns
        -------
        NoneType
            None
        str
            info.find('.//min').text
        """
        for info in self.root.findall('.//Process'):
            if info.find('.//min') is None:
                return None
            else:
                return info.find('.//min').text

    def STR_PercentMax(self):
        """
        Returns
        -------
        NoneType
            None
        str
            info.find('.//max').text
        """
        for info in self.root.findall('.//Process'):
            if info.find('.//max') is None:
                return None
            else:
                return info.find('.//max').text

    def STR_GaussSigma(self):
        """
        Returns
        -------
        str
            info.find('.//gsigma').text
        """
        for info in self.root.findall('.//Process'):
            return info.find('.//gsigma').text

    def STR_SigmaVariance(self):
        """
        Returns
        -------
        str
            info.find('.//variance').text
        """
        for info in self.root.findall('.//Process'):
            return info.find('.//variance').text

    def getBand(self):
        for info in self.root.iter('bands'):
            testband = info.findall('.//bandfilter')
            print(len(testband))
            for test in testband:
                print("test")
                print(test.text)

    def getFileListWB(self):
        """
        Returns
        -------
        list
            listArray
        """
        listArray = []
        for info in self.root.iter('ImageUrl'):
            fileUrl = info.find('url').text
            testband = info.findall('.//bandfilter')
            if len(testband) == 1:
                fileout = fileUrl + "+" + testband[0].text
            elif len(testband) == 3:
                fileout = fileUrl + "+" + \
                    testband[0].text + "," + \
                    testband[1].text + "," + testband[2].text
            else:
                fileout = fileUrl

            listArray.append(fileout)

        return listArray

    def getMFileListWB(self):
        """
        Returns
        -------
        list
            listArray
        """
        listArray = []
        for info in self.root.iter('ImageList'):
            Mfile = info.find('.//internalpath').text
            testband = info.findall('.//band')
            if len(testband) == 1:
                fileout = Mfile + "+" + testband[0].text
            elif len(testband) == 3:
                fileout = Mfile + "+" + \
                    testband[0].text + "," + \
                    testband[1].text + "," + testband[2].text
            else:
                fileout = Mfile

            listArray.append(fileout)

        return listArray

    def getFileList(self):
        """
        Returns
        -------
        list
            listArray
        """
        listArray = []
        for info in self.root.iter('ImageUrl'):
            fileUrl = info.find('url').text
            listArray.append(fileUrl)

        return listArray

def generate_pow_recipe(xmlOBJ, pds_label, MAPfile):
    with open(recipe_base + xmlOBJ.getCleanName() + '.json', 'r') as json_file:
            recipeOBJ = json.load(json_file)['pow']['recipe']

    bit_type = xmlOBJ.getOutBit().upper()

    strType = xmlOBJ.STR_Type()
    stretch_dict = {}
    stretch_dict['from_'] = list(recipeOBJ.items())[-1][-1]['to']
    stretch_dict['to'] = '{{no_extension_inputfile}}.stretch.cub'
    if strType == 'StretchPercent' and xmlOBJ.STR_PercentMin() is not None and xmlOBJ.STR_PercentMax() is not None and bit_type != 'REAL':
        if float(xmlOBJ.STR_PercentMin()) != 0 and float(xmlOBJ.STR_PercentMax()) != 100:
            if bit_type == 'UNSIGNEDBYTE':
                strpairs = '0:1 ' + xmlOBJ.STR_PercentMin() + ':1 ' + \
                    xmlOBJ.STR_PercentMax() + ':254 100:254'
            elif bit_type == 'SIGNEDWORD':
                strpairs = '0:-32765 ' + xmlOBJ.STR_PercentMin() + ':-32765 ' + \
                    xmlOBJ.STR_PercentMax() + ':32765 100:32765'

            stretch_dict['usepercentages'] = 'yes'
            stretch_dict['pairs'] = strpairs
            recipeOBJ['isis.stretch'] = stretch_dict

    elif strType == 'GaussStretch':
        stretch_dict['gsigma'] = xmlOBJ.STR_GaussSigma()
        recipeOBJ['isis.gaussstretch'] = stretch_dict

    elif strType == 'HistogramEqualization':
        if xmlOBJ.STR_PercentMin() is None:
            stretch_dict['minper'] = '0'
        else:
            stretch_dict['minper'] = xmlOBJ.STR_PercentMin()
        if xmlOBJ.STR_PercentMax() is None:
            stretch_dict['maxper'] = '100'
        else:
            stretch_dict['maxper'] = xmlOBJ.STR_PercentMax()
        recipeOBJ['isis.histeq'] = stretch_dict

    elif strType == 'SigmaStretch':
        stretch_dict['variance'] = xmlOBJ.STR_SigmaVariance()
        recipeOBJ['sigmastretch'] = stretch_dict

    if bit_type == 'UNSIGNEDBYTE' or bit_type == 'SIGNEDWORD':
        last_process = list(recipeOBJ.items())[-1][0]
        if bit_type  == 'UNSIGNEDBYTE':
            recipeOBJ[last_process]['to'] += '+lsb+tile+attached+unsignedbyte+1:254'
        elif bit_type == 'SIGNEDWORD':
            recipeOBJ[last_process]['to'] += '+lsb+tile+attached+signedword+-32765:32765'

    recipe_processes = recipeOBJ.keys()

    if 'isis.cam2map' in recipe_processes:
        recipeOBJ['isis.cam2map']['map'] = MAPfile

        if xmlOBJ.getResolution() is None:
            recipeOBJ['isis.cam2map']['pixres'] = 'CAMERA'
        else:
            recipeOBJ['isis.cam2map']['pixres'] = 'MAP'

        if xmlOBJ.getRangeType() is None:
            recipeOBJ['isis.cam2map']['defaultrange'] = 'MINIMIZE'
        elif xmlOBJ.getRangeType() == 'smart' or xmlOBJ.getRangeType() == 'fill':
            recipeOBJ['isis.cam2map']['defaultrange'] = 'CAMERA'
            recipeOBJ['isis.cam2map']['trim'] = 'YES'

    if 'isis.ctxevenodd' in recipe_processes:
        spatial_summing = pds_label.get('SAMPLING_FACTOR')
        if spatial_summing != 1:
            recipeOBJ.pop('isis.ctxevenodd')
        else:
            recipeOBJ.pop('cube_rename')

    if 'isis.mocevenodd' in recipe_processes:
        cross_track_summing = pds_label.get('CROSSTRACK_SUMMING')
        if cross_track_summing != 1:
            recipeOBJ.pop('isis.mocevenodd')
        else:
            recipeOBJ.pop('cube_rename')

    if 'isis.mocnoise50' in recipe_processes:
        cross_track_summing = pds_label.get('CROSSTRACK_SUMMING')
        if cross_track_summing != 1:
            recipeOBJ.pop('isis.mocnoise50')

    return recipeOBJ

def generate_map2_recipe(xmlOBJ, isis_label, MAPfile):

    with open(recipe_base + 'map2_process.json', 'r') as json_file:
            recipeOBJ = json.load(json_file)['map']['recipe']

    if xmlOBJ.getOutBit() == 'input':
        bit_type = str(isis_label['IsisCube']['Core']['Pixels']['Type']).upper()
    else:
        bit_type = xmlOBJ.getOutBit().upper()
    isis_pixel_type = str(isis_label['IsisCube']['Core']['Pixels']['Type']).upper()

    stretch_dict = {}
    stretch_dict['from_'] = list(recipeOBJ.items())[-1][-1]['to']
    stretch_dict['to'] = '{{no_extension_inputfile}}.stretch.cub'

    strType = xmlOBJ.STR_Type()
    if xmlOBJ.getProcess() == 'MAP2' and strType is None:
        if bit_type != isis_pixel_type and bit_type != 'REAL':

            if bit_type == 'SIGNEDWORD':
                stretch_pairs = '0:-32765 0:-32765 100:32765 100:32765'
            elif bit_type == 'UNSIGNEDBYTE':
                stretch_pairs = '0:1 0:1 100:254 100:254'
            stretch_dict['pairs'] = stretch_pairs
            stretch_dict['usepercentages'] = 'yes'
            recipeOBJ['isis.stretch'] = stretch_dict

    if strType == 'StretchPercent' and xmlOBJ.STR_PercentMin() is not None and xmlOBJ.STR_PercentMax() is not None and bit_type != 'REAL':
        if float(xmlOBJ.STR_PercentMin()) != 0 and float(xmlOBJ.STR_PercentMax()) != 100:
            if bit_type == 'UNSIGNEDBYTE':
                stretch_pairs = '0:1 ' + xmlOBJ.STR_PercentMin() + ':1 ' + \
                    xmlOBJ.STR_PercentMax() + ':254 100:254'
            elif bit_type == 'SIGNEDWORD':
                stretch_pairs = '0:-32765 ' + xmlOBJ.STR_PercentMin() + ':-32765 ' + \
                    xmlOBJ.STR_PercentMax() + ':32765 100:32765'

            stretch_dict['usepercentages'] = 'yes'
            stretch_dict['pairs'] = stretch_pairs
            recipeOBJ['isis.stretch'] = stretch_dict

    elif strType == 'GaussStretch':
        stretch_dict['gsigma'] = xmlOBJ.STR_GaussSigma()
        recipeOBJ['isis.gaussstretch'] = stretch_dict

    elif strType == 'HistogramEqualization':
        if xmlOBJ.STR_PercentMin() is None:
            stretch_dict['minper'] = '0'
        else:
            stretch_dict['minper'] = xmlOBJ.STR_PercentMin()
        if xmlOBJ.STR_PercentMax() is None:
            stretch_dict['maxper'] = '100'
        else:
            stretch_dict['maxper'] = xmlOBJ.STR_PercentMax()
        recipeOBJ['isis.histeq'] = stretch_dict

    elif strType == 'SigmaStretch':
        stretch_dict['variance'] = xmlOBJ.STR_SigmaVariance()
        recipeOBJ['isis.sigmastretch'] = stretch_dict

    if xmlOBJ.getGridInterval() is not None:
        grid_dict = {}
        grid_dict['from_'] = list(recipeOBJ.items())[-1][-1]['to'].split('+')[0]
        grid_dict['to'] = '{{no_extension_inputfile}}.grid.cub'
        grid_dict['latinc'] = xmlOBJ.getGridInterval()
        grid_dict['loninc'] = xmlOBJ.getGridInterval()
        grid_dict['outline'] = 'yes'
        grid_dict['boundary'] = 'yes'
        grid_dict['linewidth'] = '3'
        recipeOBJ['isis.grid'] = grid_dict

    if bit_type != 'INPUT':
        if bit_type == 'UNSIGNEDBYTE' or bit_type == 'SIGNEDWORD':
            if bit_type != isis_pixel_type:
                last_process = list(recipeOBJ.items())[-1][0]
                if bit_type == 'UNSIGNEDBYTE':
                    recipeOBJ[last_process]['to'] += '+lsb+tile+attached+unsignedbyte+1:254'
                elif bit_type == 'SIGNEDWORD':
                    recipeOBJ[last_process]['to'] += '+lsb+tile+attached+signedword+-32765:32765'
    
    if 'isis.map2map' in recipeOBJ.keys():
        recipeOBJ['isis.map2map']['map'] = MAPfile

        if xmlOBJ.getResolution() is None:
            recipeOBJ['isis.map2map']['pixres'] = 'FROM'
        else:
            recipeOBJ['isis.map2map']['pixres'] = 'MAP'

        if xmlOBJ.OutputGeometry() is not None:
            recipeOBJ['isis.map2map']['defaultrange'] = 'MAP'
            recipeOBJ['isis.map2map']['trim'] = 'YES'
        else:
            recipeOBJ['isis.map2map']['defaultrange'] = 'FROM'

    return recipeOBJ

def parse_args():
    parser = argparse.ArgumentParser(description='Service job manager')
    parser.add_argument('--key',
                        '-k',
                        dest='key',
                        help="Target key -- if blank, process first element in queue")
    parser.add_argument('--namespace',
                        '-n',
                        dest='namespace',
                        help="Queue namespace")
    parser.add_argument('--norun',
                        help="Set up queues and write out SBATCH script, but do not submit it to SLURM",
                        action="store_true")

    args = parser.parse_args()
    return args


def main(user_args):
    key = user_args.key
    norun = user_args.norun
    namespace = user_args.namespace

    if namespace is None:
        namespace = default_namespace

    # Set up logging
    logger = logging.getLogger(key)
    logger.setLevel(logging.INFO)

    logFileHandle = logging.FileHandler(pds_log + 'Service.log')
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s, %(message)s')
    logFileHandle.setFormatter(formatter)
    logger.addHandler(logFileHandle)
    RQ_lock = RedisLock(lock_obj)
    RQ_lock.add({'Services':'1'})

    if not RQ_lock.available('Services'):
        exit()

    # Connect to database and access 'jobs' table
    DBQO = PDS_DBquery('JOBS')
    if key is None:
        # If no key is specified, grab the first key
        try:
            key = DBQO.jobKey()
            try:
                key = key.decode('utf-8')
            except:
                pass
        # If the queue is empty, it'll throw a type error.
        except TypeError:
            logger.debug('No keys found in clusterjobs database')
            exit(1)
    try:
        # Set the 'queued' column to current time i.e. prep for processing
        DBQO.setJobsQueued(key)
    except KeyError as e:
        logger.error('%s', e)
        exit(1)

    logger.info('Starting Process')

    xmlOBJ = jobXML(DBQO.jobXML4Key(key))

    # Make directory if it doesn't exist
    directory = os.path.join(workarea, key)
    if not os.path.exists(directory):
        os.makedirs(directory)

    logger.info('Working Area: %s', directory)


    # Set up Redis Hash for ground range
    RedisH = RedisHash(key + '_info')
    RedisH.RemoveAll()
    RedisErrorH = RedisHash(key + '_error')
    RedisErrorH.RemoveAll()
    RedisH_DICT = {}
    RedisH_DICT['service'] = xmlOBJ.getProcess()
    RedisH_DICT['fileformat'] = xmlOBJ.getOutFormat()
    RedisH_DICT['outbit'] = xmlOBJ.getOutBit()
    if xmlOBJ.getRangeType() is not None:
        RedisH_DICT['grtype'] = xmlOBJ.getRangeType()
        RedisH_DICT['minlat'] = xmlOBJ.getMinLat()
        RedisH_DICT['maxlat'] = xmlOBJ.getMaxLat()
        RedisH_DICT['minlon'] = xmlOBJ.getMinLon()
        RedisH_DICT['maxlon'] = xmlOBJ.getMaxLon()

    if RedisH.IsInHash('service'):
        pass
    else:
        RedisH.AddHash(RedisH_DICT)
    if RedisH.IsInHash('service'):
        logger.info('Redis info Hash: Success')
    else:
        logger.error('Redis info Hash Not Found')

    # End ground range

    RQ_recipe = RedisQueue(key + '_recipe', namespace)
    RQ_recipe.RemoveAll()
    RQ_file = RedisQueue(key + '_FileQueue', namespace)
    RQ_file.RemoveAll()
    RQ_WorkQueue = RedisQueue(key + '_WorkQueue', namespace)
    RQ_WorkQueue.RemoveAll()
    RQ_loggy = RedisQueue(key + '_loggy', namespace)
    RQ_loggy.RemoveAll()
    RQ_zip = RedisQueue(key + '_ZIP', namespace)
    RQ_zip.RemoveAll()

    if xmlOBJ.getProcess() == 'POW':
        fileList = xmlOBJ.getFileListWB()
    elif xmlOBJ.getProcess() == 'MAP2':
        fileList = xmlOBJ.getMFileListWB()

    for List_file in fileList:

        # Input and output file naming and path stuff
        if xmlOBJ.getProcess() == 'POW':
            if xmlOBJ.getInst() == 'THEMIS_IR':
                Input_file = List_file.replace('odtie1_', 'odtir1_')
                Input_file = Input_file.replace('xxedr', 'xxrdr')
                Input_file = Input_file.replace('EDR.QUB', 'RDR.QUB')
                Input_file = Input_file.replace(
                    'http://pdsimage.wr.usgs.gov/Missions/', archive_base)
            elif xmlOBJ.getInst() == 'ISSNA':
                Input_file = List_file.replace('.IMG', '.LBL')
                Input_file = Input_file.replace(
                    'http://pdsimage.wr.usgs.gov/Missions/', archive_base)
            elif xmlOBJ.getInst() == 'ISSWA':
                Input_file = List_file.replace('.IMG', '.LBL')
                Input_file = Input_file.replace(
                    'http://pdsimage.wr.usgs.gov/Missions/', archive_base)
            elif xmlOBJ.getInst() == 'SOLID STATE IMAGING SYSTEM':
                Input_file = List_file.replace('.img', '.lbl')
                Input_file = Input_file.replace(
                    'http://pdsimage.wr.usgs.gov/Missions/', archive_base)
            else:
                Input_file = List_file.replace(
                    'http://pdsimage.wr.usgs.gov/Missions/', archive_base)

        elif xmlOBJ.getProcess() == 'MAP2':
            Input_file = List_file.replace('file://pds_san', '/pds_san')

            if '+' in Input_file:
                tempsplit = Input_file.split('+')
                tempFile = tempsplit[0]
            else:
                tempFile = Input_file

            # Output final file naming
            Tbasename = os.path.splitext(os.path.basename(tempFile))[0]
            splitBase = Tbasename.split('_')

            labP = xmlOBJ.getProjection()
            isis_label = pvl.load(tempFile)
            if labP == 'INPUT':
                lab_proj = isis_label['IsisCube']['Mapping']['ProjectionName'][0:4]
            else:
                lab_proj = labP[0:4]

            if xmlOBJ.getClat() is None or xmlOBJ.getClon() is None:
                basefinal = splitBase[0] + splitBase[1] + \
                    splitBase[2] + '_MAP2_' + lab_proj.upper()
            else:
                lab_clat = float(xmlOBJ.getClat())
                if lab_clat >= 0:
                    labH = 'N'
                elif lab_clat < 0:
                    labH = 'S'
                lab_clon = float(xmlOBJ.getClon())

                basefinal = splitBase[0] + splitBase[1] + splitBase[2] + '_MAP2_' + str(
                    lab_clat) + labH + str(lab_clon) + '_' + lab_proj.upper()
            RedisH.MAPname(basefinal)

        try:
            basename = os.path.splitext(os.path.basename(Input_file))[0]
            RQ_file.QueueAdd(Input_file)
            logger.info('File %s Added to Redis Queue', Input_file)
        except Exception as e:
            logger.warn('File %s NOT Added to Redis Queue', Input_file)
            print('Redis Queue Error', e)
    RedisH.FileCount(RQ_file.QueueSize())
    logger.info('Count of Files Queue: %s', str(RQ_file.QueueSize()))

    # Map Template Stuff
    logger.info('Making Map File')
    mapOBJ = MakeMap()

    if xmlOBJ.getProcess() == 'MAP2' and xmlOBJ.getProjection() == 'INPUT':
        proj = isis_label['IsisCube']['Mapping']['ProjectionName']
        mapOBJ.Projection(proj)
    else:
        mapOBJ.Projection(xmlOBJ.getProjection())

    if xmlOBJ.getClon() is not None:
        mapOBJ.CLon(float(xmlOBJ.getClon()))
    if xmlOBJ.getClat() is not None:
        mapOBJ.CLat(float(xmlOBJ.getClat()))
    if xmlOBJ.getFirstParallel() is not None:
        mapOBJ.FirstParallel(float(xmlOBJ.getFirstParallel()))
    if xmlOBJ.getSecondParallel() is not None:
        mapOBJ.SecondParallel(float(xmlOBJ.getSecondParallel()))
    if xmlOBJ.getResolution() is not None:
        mapOBJ.PixelRes(float(xmlOBJ.getResolution()))
    if xmlOBJ.getTargetName() is not None:
        mapOBJ.Target(xmlOBJ.getTargetName())
    if xmlOBJ.getERadius() is not None:
        mapOBJ.ERadius(float(xmlOBJ.getERadius()))
    if xmlOBJ.getPRadius() is not None:
        mapOBJ.PRadius(float(xmlOBJ.getPRadius()))
    if xmlOBJ.getLatType() is not None:
        mapOBJ.LatType(xmlOBJ.getLatType())
    if xmlOBJ.getLonDirection() is not None:
        mapOBJ.LonDirection(xmlOBJ.getLonDirection())
    if xmlOBJ.getLonDomain() is not None:
        mapOBJ.LonDomain(int(xmlOBJ.getLonDomain()))

    if xmlOBJ.getProcess() == 'MAP2':
        if xmlOBJ.getMinLat() is not None:
            mapOBJ.MinLat(float(xmlOBJ.getMinLat()))
        if xmlOBJ.getMaxLat() is not None:
            mapOBJ.MaxLat(float(xmlOBJ.getMaxLat()))
        if xmlOBJ.getMinLon() is not None:
            mapOBJ.MinLon(float(xmlOBJ.getMinLon()))
        if xmlOBJ.getMaxLon() is not None:
            mapOBJ.MaxLon(float(xmlOBJ.getMaxLon()))

    mapOBJ.Map2pvl()

    MAPfile = directory + "/" + key + '.map'
    mapOBJ.Map2File(MAPfile)

    try:
        f = open(MAPfile)
        f.close
        logger.info('Map File Creation: Success')
    except IOError as e:
        logger.error('Map File %s Not Found', MAPfile)

    # ** End Map Template Stuff **

    logger.info('Building Recipe')
    if xmlOBJ.getProcess() == 'POW':
        pds_label = pvl.load(Input_file.split('+')[0])
        recipeOBJ = generate_pow_recipe(xmlOBJ, pds_label, MAPfile)

    elif xmlOBJ.getProcess() == 'MAP2':
        recipeOBJ = generate_map2_recipe(xmlOBJ, isis_label, MAPfile)

    # OUTPUT FORMAT
    # Test for GDAL and add to recipe
    Oformat = xmlOBJ.getOutFormat()
    if Oformat == 'GeoTiff-BigTiff' or Oformat == 'GeoJPEG-2000' or Oformat == 'JPEG' or Oformat == 'PNG':
        if Oformat == 'GeoJPEG-2000':
            Oformat = 'JP2KAK'
        if Oformat == 'GeoTiff-BigTiff':
            Oformat = 'GTiff'
        gdal_translate_dict = {}

        def GDAL_OBit(ibit):
            bitDICT = {'unsignedbyte': 'Byte',
                       'signedword': 'Int16',
                       'real': 'Float32'
                       }
            try:
                return bitDICT[ibit]
            except KeyError:
                raise Exception(f"Unsupported ibit type given {ibit}. " +
                                f"Currently supported bit types are {list(bitDICT.keys())}")
        def GDAL_Creation(format):
            cDICT = {'JPEG': 'quality=100',
                     'JP2KAK': 'quality=100',
                     'GTiff': 'bigtiff=if_safer'
                     }
            try:
                return cDICT[format]
            except KeyError:
                raise Exception(f"Unsupported format {format}. " +
                                f"Currently supported bit types are {list(cDICT.keys())}")

        if xmlOBJ.getOutBit() != 'input':
            gdal_translate_dict['outputType'] = GDAL_OBit(xmlOBJ.getOutBit())
        gdal_translate_dict['format'] = Oformat

        if Oformat == 'GTiff' or Oformat == 'JP2KAK' or Oformat == 'JPEG':
            gdal_translate_dict['creationOptions'] = [GDAL_Creation(Oformat)]

        frmt = xmlOBJ.getOutFormat()
        if frmt == 'GeoTiff-BigTiff':
            fileext = 'tif'
        elif frmt == 'GeoJPEG-2000':
            fileext = 'jp2'
        elif frmt == 'JPEG':
            fileext = 'jpg'
        elif frmt == 'PNG':
            fileext = 'png'
        elif frmt == 'GIF':
            fileext = 'gif'

        gdal_translate_dict['src'] = list(recipeOBJ.items())[-1][-1]['to'].split('+')[0]
        gdal_translate_dict['dest'] = "{{no_extension_inputfile}}_final." + fileext

        recipeOBJ['gdal_translate'] = gdal_translate_dict
    # set up pds2isis and add to recipe
    elif Oformat == 'PDS':
        isis2pds_dict = {}
        isis2pds_dict['from_'] = list(recipeOBJ.items())[-1][-1]['to']
        isis2pds_dict['to'] = "{{no_extension_inputfile}}_final.img"
        if xmlOBJ.getOutBit() == 'unsignedbyte':
            isis2pds_dict['bittype'] = '8bit'
        elif xmlOBJ.getOutBit() == 'signedword':
            isis2pds_dict['bittype'] = 's16bit'
        recipeOBJ['isis.isis2pds'] = isis2pds_dict

    try:
        RQ_recipe.QueueAdd(json.dumps(recipeOBJ))
        logger.info('Recipe Added to Redis')
    except Exception as e:
        logger.warn('Recipe NOT Added to Redis: %s', recipeOBJ)

    # HPC job stuff
    logger.info('HPC Cluster job Submission Starting')
    jobOBJ = HPCjob()
    jobOBJ.setJobName(key + '_Service')
    jobOBJ.setStdOut(slurm_log + key + '_%A_%a.out')
    jobOBJ.setStdError(slurm_log + key + '_%A_%a.err')
    jobOBJ.setWallClock('24:00:00')
    jobOBJ.setMemory('24576')
    jobOBJ.setPartition('pds')
    JAsize = RQ_file.QueueSize()
    jobOBJ.setJobArray(JAsize)
    logger.info('Job Array Size : %s', str(JAsize))

    # @TODO replace with source activate <env>
    #jobOBJ.addPath('/usgs/apps/anaconda/bin')

    # Whether or not we use the default namespace, this guarantees that the POW/MAP queues will match the namespace
    #  used in the job manager.
    if xmlOBJ.getProcess() == 'POW':
        cmd = cmd_dir + "pow_process.py -k {} -n {}".format(key, namespace)
    elif xmlOBJ.getProcess() == 'MAP2':
        cmd = cmd_dir + "map_process.py -k {} -n {}".format(key, namespace)

    logger.info('HPC Command: %s', cmd)
    jobOBJ.setCommand(cmd)

    SBfile = directory + '/' + key + '.sbatch'
    jobOBJ.MakeJobFile(SBfile)

    try:
        sb = open(SBfile)
        sb.close
        logger.info('SBATCH File Creation: Success')
    except IOError as e:
        logger.error('SBATCH File %s Not Found', SBfile)


    if norun:
        logger.info('No-run mode, will not submit HPC job.')
    else:
        try:
            jobOBJ.Run()
            logger.info('Job Submission to HPC: Success')
            DBQO.setJobsStarted(key)
        except IOError as e:
            logger.error('Jobs NOT Submitted to HPC')


if __name__ == "__main__":
    sys.exit(main(parse_args()))
