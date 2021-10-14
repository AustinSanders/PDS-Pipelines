import datetime
import json
import pytz
from pvl import PVLModule
import logging

import pytest
from shapely.geometry import Polygon
from geoalchemy2.elements import WKBElement
from geoalchemy2.shape import from_shape, to_shape
import numpy as np
from unittest.mock import patch, PropertyMock, Mock
from unittest import mock

import pysis
from pysis import isis
from pysis.exceptions import ProcessError

# Stub in the getsn function for testing
def getsn(from_):
    return b'ISISSERIAL'
pysis.isis.getsn = getsn

# Stub in the getkey function for testing
def getkey(from_, objname=None, grp=None, keyword=None, keyindex=None, upper=False, recursive=True):
    return b'PRODUCTID'
pysis.isis.getkey = getkey

# Stub in the getsn function for testing
def spiceinit(from_):
    raise ProcessError(1, ['spiceinit'], b'Could not spiceinit', b'Could not spiceinit')
pysis.isis.spiceinit = spiceinit

import pds_pipelines.models.upc_models as models
from pds_pipelines.process import Process
from pds_pipelines import utils
from pds_pipelines.upc_update import get_target_name, get_instrument_name, \
                                      get_spacecraft_name, create_datafiles_atts, \
                                      create_search_terms_atts, create_json_keywords_atts, \
                                      getPDSid

from pds_pipelines.utils import process, generate_processes, get_isis_id
from pds_pipelines.config import recipe_base

@pytest.fixture
def pds_label():
    return PVLModule({'^IMAGE': ('5600R.IMG', 12),
                      'SPACECRAFT_NAME': 'TEST CRAFT',
                      'INSTRUMENT_NAME': 'TEST INSTRUMENT',
                      'TARGET_NAME': 'TEST TARGET'})

cam_info_dict = {'upcid': 1,
                 'processdate': datetime.datetime.now(pytz.utc).strftime("%Y-%m-%d %H"),
                 'starttime': '2019-12-18 21',
                 'solarlongitude': 66.210772203754,
                 'meangroundresolution': 4.409120557106079,
                 'minimumemission': 4.541800232653493,
                 'maximumemission': 8.541800232653493,
                 'emissionangle': 6.541800232653493,
                 'minimumincidence': 61.821788119170066,
                 'maximumincidence': 65.82178811917007,
                 'incidenceangle': 63.821788119170066,
                 'minimumphase': 76.58058078942486,
                 'maximumphase': 80.58058078942486,
                 'phaseangle': 78.58058078942486,
                 'targetid': 1,
                 'instrumentid': 1,
                 'pdsproductid': 'PRODUCTID',
                 'err_flag': False,
                 'gisfootprint': Polygon([(153.80256122853893, -32.68515128444211),
                                           (153.80256122853893, -33.18515128444211),
                                           (153.30256122853893, -33.18515128444211),
                                           (153.30256122853893, -32.68515128444211),
                                           (153.80256122853893, -32.68515128444211)]).wkt}

def test_get_pds_id():
    prod_id = getPDSid('/Path/to/my/cube.cub')
    assert isinstance(prod_id, str)
    assert prod_id == 'PRODUCTID'

def test_get_isis_id():
    cube_path = '/Path/to/my/cube.cub'
    serial = get_isis_id(cube_path)
    assert serial == 'ISISSERIAL'
    assert isinstance(serial, str)

def test_target_name(pds_label):
    target_name = get_target_name(pds_label)
    assert pds_label['TARGET_NAME'] == target_name

def test_bad_target_name():
    target_name = get_target_name(PVLModule())
    assert target_name == None

def test_instrument_name(pds_label):
    instrument_name = get_instrument_name(pds_label)
    assert pds_label['INSTRUMENT_NAME'] == instrument_name

def test_spacecraft_name(pds_label):
    spacecraft_name = get_spacecraft_name(pds_label)
    assert pds_label['SPACECRAFT_NAME'] == spacecraft_name

def test_bad_instrument_name():
    instrument_name = get_instrument_name(PVLModule())
    assert instrument_name == None

def test_bad_spacecraft_name():
    spacecraft_name = get_spacecraft_name(PVLModule())
    assert spacecraft_name == None

@patch('pds_pipelines.upc_update.get_isis_id', return_value = 'ISISSERIAL')
@patch('pds_pipelines.upc_update.getPDSid', return_value = 'PRODUCTID')
def test_datafile_generation(mocked_pds_id, mocked_isis_id, pds_label):
    input_cube = '/Path/to/my/cube.cub'
    datafile_attributes = create_datafiles_atts(pds_label, '/Path/to/label/location/label.lbl', input_cube)
    mocked_isis_id.assert_called_with(input_cube)
    mocked_pds_id.assert_called_with(input_cube)

    expected_attributes = {'upcid': None, 'isisid': 'ISISSERIAL', 'productid': 'PRODUCTID',
                           'source': '/Path/to/label/location/label.img',
                           'detached_label': '/Path/to/label/location/label.lbl',
                           'instrumentid': None, 'targetid': None, 'level': None}
    shared_items = {k: expected_attributes[k] for k in expected_attributes if k in datafile_attributes and expected_attributes[k] == datafile_attributes[k]}
    assert len(shared_items) == 8

@patch('pds_pipelines.upc_update.get_isis_id', return_value = 'ISISSERIAL')
@patch('pds_pipelines.upc_update.getPDSid', return_value = 'PRODUCTID')
def test_datafiles_no_label(mocked_pds_id, mocked_isis_id, pds_label):
    pds_label['^IMAGE'] = 1
    datafile_attributes = create_datafiles_atts(pds_label, '/Path/to/label/location/label.lbl', '/Path/to/my/cube.cub')

    assert datafile_attributes['detached_label'] == None
    assert datafile_attributes['source'] == '/Path/to/label/location/label.lbl'

@patch('pds_pipelines.upc_update.getPDSid', return_value = 'PRODUCTID')
def test_datafiles_no_isisid(mocked_pds_id, pds_label):
    # Since we mock getsn above, make it throw an exception here so we can test
    # when there is no ISIS ID.
    with patch('pysis.isis.getsn', side_effect=ProcessError(1, ['getsn'], '', '')):
        datafile_attributes = create_datafiles_atts(pds_label, '/Path/to/label/location/label.lbl', '/Path/to/my/cube.cub')
    assert datafile_attributes['isisid'] == None

@patch('pds_pipelines.upc_update.get_isis_id', return_value = 'ISISSERIAL')
def test_datafiles_no_pdsid(mocked_isis_id, pds_label):
    # Since we mock getkey above, make it throw an exception here so we can test
    # when there is no PDS ID.
    with patch('pysis.isis.getkey', side_effect=ProcessError(1, 'getkey', '', '')):
        datafile_attributes = create_datafiles_atts(pds_label, '/Path/to/label/location/label.lbl', '/Path/to/my/cube.cub')
    assert datafile_attributes['productid'] == None

def extract_keyword(pvl, key):
    return cam_info_dict[key]

@patch('pds_pipelines.upc_update.find_keyword', side_effect = extract_keyword)
@patch('pds_pipelines.upc_update.getPDSid', return_value = 'PRODUCTID')
def test_search_terms_generation(mocked_product_id, mocked_keyword):
    upc_id = cam_info_dict['upcid']
    with patch('pds_pipelines.upc_update.load_pvl', return_value = pds_label):
        search_term_attributes = create_search_terms_atts('/Path/to/caminfo.pvl', upc_id, '/Path/to/my/cube.cub', '')
    # Convert the dates from strings back to date times. This could probably
    # be handled in the model
    search_term_attributes['processdate'] = datetime.datetime.strptime(search_term_attributes['processdate'], "%Y-%m-%d %H:%M:%S")
    search_term_attributes['starttime'] = datetime.datetime.strptime(search_term_attributes['starttime'], "%Y-%m-%d %H")

    search_term_mapping = dict(zip(search_term_attributes.keys(), search_term_attributes.keys()))
    search_term_mapping['isisfootprint'] = 'gisfootprint'

    for key in search_term_mapping.keys():
        attribute = search_term_attributes[key]
        if isinstance(attribute, datetime.date):
            attribute = attribute.strftime("%Y-%m-%d %H")
        if isinstance(attribute, WKBElement):
            attribute = str(to_shape(attribute))
        if isinstance(attribute, float):
            np.testing.assert_almost_equal(attribute, cam_info_dict[key], 12)
            continue
        assert cam_info_dict[search_term_mapping[key]] == attribute

@patch('pds_pipelines.upc_update.getPDSid', return_value = 'PRODUCTID')
def test_search_terms_keyword_exception(mocked_product_id, pds_label):
    upc_id = cam_info_dict['upcid']

    search_term_attributes = create_search_terms_atts("", upc_id, '/Path/to/my/cube.cub', '')
    assert search_term_attributes['starttime'] == None
    assert search_term_attributes['solarlongitude'] == None
    assert search_term_attributes['meangroundresolution'] == None
    assert search_term_attributes['minimumemission'] == None
    assert search_term_attributes['maximumemission'] == None
    assert search_term_attributes['emissionangle'] == None
    assert search_term_attributes['minimumincidence'] == None
    assert search_term_attributes['maximumincidence'] == None
    assert search_term_attributes['incidenceangle'] == None
    assert search_term_attributes['minimumphase'] == None
    assert search_term_attributes['maximumphase'] == None
    assert search_term_attributes['phaseangle'] == None
    assert search_term_attributes['isisfootprint'] == None
    assert search_term_attributes['err_flag'] == True

def test_json_keywords_generation(pds_label):
    logger = logging.getLogger('UPC_Process')
    upc_id = cam_info_dict['upcid']

    with patch('pds_pipelines.upc_update.load_pvl', return_value = pds_label):
        json_keywords_attributes = create_json_keywords_atts(pds_label, upc_id, '/Path/to/my/cube.cub', 'No Failures', logger)

    result_json = json_keywords_attributes['jsonkeywords']
    assert result_json['^IMAGE'][0] == pds_label['^IMAGE'][0]
    assert result_json['TARGET_NAME'] == pds_label['TARGET_NAME']
    assert result_json['INSTRUMENT_NAME'] == pds_label['INSTRUMENT_NAME']
    assert result_json['SPACECRAFT_NAME'] == pds_label['SPACECRAFT_NAME']

def test_json_keywords_exception():
    logger = logging.getLogger('UPC_Process')
    upc_id = cam_info_dict['upcid']

    input_cube = '/Path/to/my/cube.cub'
    error_message = 'Got to exception.'
    json_keywords_attributes = create_json_keywords_atts(None, upc_id, input_cube, error_message, logger)
    print(json_keywords_attributes)

    result_json = json_keywords_attributes['jsonkeywords']
    assert result_json['errortype'] == error_message
    assert result_json['file'] == input_cube
    assert result_json['errormessage'] == f'Error running {error_message} on file {input_cube}'
    assert result_json['error'] == True

def test_generate_processes():
    logger = logging.getLogger('UPC_Process')

    inputfile = "./pds_pipelines/tests/data/5600r.lbl"
    fid = "1"
    recipe_file = recipe_base + "/galileo_ssi_edr.json"
    with open(recipe_file) as fp:
        original_recipe = json.load(fp)['upc']['recipe']
        recipe_string = json.dumps(original_recipe)

    processes = generate_processes(inputfile, recipe_string, logger)

    for k, v in processes.items():
        assert original_recipe[k].keys() == v.keys()

def test_process_isis():
    logger = logging.getLogger('UPC_Process')

    processes = {'isis.getsn': {'from_': '/Path/to/some/cube.cub'}}

    failing_command, error = process(processes, '/', logger)
    assert failing_command == ''
    assert error == ''

def test_bad_process_isis():
    logger = logging.getLogger('UPC_Process')

    processes = {'isis.spiceinit': {'from_': '/Path/to/some/cube.cub'}}

    failing_command, error = process(processes, '/', logger)
    assert failing_command == 'spiceinit'
    assert error != ''
