import os
import jinja2
import json
import pds_pipelines
from pds_pipelines.available_modules import *
from pysis.exceptions import ProcessError
from collections import defaultdict

class MultiDict(dict):
    """ Helper object for parse_pairs.  Normal dictionary, but adds values to list instead of overwriting duplicate keys.
    """
    def __setitem__(self, key, value):
        if key in self:
            try:
                self[key].append(value)
            except AttributeError:
                super().__setitem__(key, [self[key], value])
        else:
            super().__setitem__(key, value)

"""
JSON object_pairs_hook target.  Necessary to allow duplicate key specification from JSON documents to Python.
Uses the MultiDict defined above instead of default python dictionary.
"""
def parse_pairs(pairs):
    dct = MultiDict()
    for k, v in pairs:
        dct[k] = v
    return dct


def generate_processes(inputfile, recipe_string, logger, **kwargs):
    logger.info('Starting Process: %s', inputfile)
    # Working directory for processing should be same as inputfile
    workarea_pwd = os.path.dirname(inputfile)

    logger.debug("Beginning processing on %s\n", inputfile)
    no_extension_inputfile = os.path.splitext(inputfile)[0]
    cam_info_file = no_extension_inputfile + '_caminfo.pvl'
    footprint_file = no_extension_inputfile + '_footprint.json'

    template = jinja2.Template(recipe_string)
    recipe_str = template.render(inputfile=inputfile,
                                 no_extension_inputfile=no_extension_inputfile,
                                 cam_info_file=cam_info_file,
                                 footprint_file=footprint_file,
                                 **kwargs)
    processes = json.loads(recipe_str)
    return processes, no_extension_inputfile, cam_info_file, footprint_file, workarea_pwd


def process(processes, workarea_pwd, logger):
    # iterate through functions from the processes dictionary
    failing_command = ''
    for process, keywargs in processes.items():
        try:
            module, command = process.split('.')
            # load a function into func
            func = getattr(available_modules[module], command)
        except ValueError:
            func = getattr(pds_pipelines.available_modules, process)
            command = process
        try:
            os.chdir(workarea_pwd)
            # execute function
            logger.debug("Running %s", process)
            if type(keywargs) is list:
                [func(**parameterization) for parameterization in keywargs]
            else:
                func(**keywargs)

        except ProcessError as e:
            logger.debug("%s", e)
            logger.debug("%s", e.stderr)
            failing_command = command
            break

    return failing_command


def getISISid(infile):
    """ Use ISIS to get the serial number of a file.

    Parameters
    ----------
    infile : str
        A string file path for which the serial number will be calculated.


    Returns
    -------
    newisisSerial : str
        The serial number of the input file.
    """
    try:
        serial_num = isis.getsn(from_=infile)
    except (ProcessError, KeyError):
        # If either isis was not imported or a serial number could not be
        # generated from the infile set the serial number to an empty string
        return None

    # in later versions of getsn, serial_num is returned as bytes
    if isinstance(serial_num, bytes):
        serial_num = serial_num.decode()
    newisisSerial = serial_num.replace('\n', '')
    return newisisSerial



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
    : str
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
