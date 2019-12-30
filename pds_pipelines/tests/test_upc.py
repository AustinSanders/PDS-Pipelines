from datetime import datetime
import json
from pvl import PVLModule

import pytest
import sqlalchemy
from shapely.geometry import Polygon
from geoalchemy2.elements import WKBElement
from geoalchemy2.shape import from_shape, to_shape
import numpy as np
from unittest.mock import patch, PropertyMock
from unittest import mock

import pysis
from pysis import isis
from pysis.exceptions import ProcessError

# Stub in the getsn function for testing
def getsn(from_):
    return b'ISISSERIAL'
pysis.isis.getsn = getsn

import pds_pipelines
from pds_pipelines.db import db_connect
from pds_pipelines import UPCprocess
from pds_pipelines.UPCprocess import *

from pds_pipelines.UPCprocess import create_datafiles_record, create_search_terms_record, create_json_keywords_record, get_target_id, getISISid, process_isis, generate_isis_processes
from pds_pipelines.models import upc_models as model

@pytest.fixture
def tables():
    _, engine = db_connect('test')
    return engine.table_names()

@pytest.fixture
def session(tables, request):
    Session, _ = db_connect('test')
    session = Session()

    def cleanup():
        session.rollback()  # Necessary because some tests intentionally fail
        for t in reversed(tables):
            # Skip the srid table
            if t != 'spatial_ref_sys':
                session.execute(f'TRUNCATE TABLE {t} CASCADE')
            # Reset the autoincrementing
            if t in ['datafiles', 'instruments', 'targets']:
                if t == 'datafiles':
                    column = f'{t}_upcid_seq'
                if t == 'instruments':
                    column = f'{t}_instrumentid_seq'
                if t == 'targets':
                    column = f'{t}_targetid_seq'

                session.execute(f'ALTER SEQUENCE {column} RESTART WITH 1')
        session.commit()

    request.addfinalizer(cleanup)

    return session

@pytest.fixture
def session_maker(tables, request):
    Session, _ = db_connect('test')
    return Session

@pytest.fixture
def pds_label():
    return PVLModule({'^IMAGE': ('5600R.IMG', 12),
                      'SPACECRAFT_NAME': 'TEST CRAFT',
                      'INSTRUMENT_NAME': 'TEST INSTRUMENT',
                      'TARGET_NAME': 'TEST TARGET'})

cam_info_dict = {'upcid': 1,
                 'processdate': datetime.datetime.now(pytz.utc).strftime("%Y-%m-%d %H:%M"),
                 'starttime': '2019-12-18 21:36',
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
                 'isisfootprint': Polygon([(153.80256122853893, -32.68515128444211),
                                           (153.80256122853893, -33.18515128444211),
                                           (153.30256122853893, -33.18515128444211),
                                           (153.30256122853893, -32.68515128444211),
                                           (153.80256122853893, -32.68515128444211)]).wkt}

def test_datafiles_exists(tables):
    assert model.DataFiles.__tablename__ in tables

def test_instruments_exists(tables):
    assert model.Instruments.__tablename__ in tables

def test_targets_exists(tables):
    assert model.Targets.__tablename__ in tables

def test_search_terms_exists(tables):
    assert model.SearchTerms.__tablename__ in tables

def test_json_keywords_exists(tables):
    assert model.JsonKeywords.__tablename__ in tables

def test_target_insert(session, session_maker, pds_label):
    target_id = get_target_id(pds_label, session_maker)
    resp = session.query(model.Targets).filter(model.Targets.targetid==target_id).one()
    target_name = resp.targetname
    assert pds_label['TARGET_NAME'] == target_name

def test_bad_target_insert(session, session_maker):
    target_id = get_target_id(PVLModule(), session_maker)
    assert target_id == None
    with pytest.raises(sqlalchemy.orm.exc.NoResultFound):
        session.query(model.Targets).filter(model.Targets.targetid==target_id).one()

def test_instrument_insert(session, session_maker, pds_label):
    instrument_id = get_instrument_id(pds_label, session_maker)
    resp = session.query(model.Instruments).filter(model.Instruments.instrumentid == instrument_id).one()
    instrument_name = resp.instrument
    assert pds_label['INSTRUMENT_NAME'] == instrument_name

def test_bad_instrumentname_instrument_insert(session, session_maker):
    instrument_id = get_instrument_id(PVLModule(), session_maker)
    assert instrument_id == None
    with pytest.raises(sqlalchemy.orm.exc.NoResultFound):
        session.query(model.Instruments).filter(model.Instruments.instrumentid == instrument_id).one()

def test_bad_spacecraftname_instrument_insert(session, session_maker):
    instrument_id = get_instrument_id(PVLModule({'INSTRUMENT_NAME': 'TEST INSTRUMENT'}), session_maker)
    assert instrument_id == None
    with pytest.raises(sqlalchemy.orm.exc.NoResultFound):
        session.query(model.Instruments).filter(model.Instruments.instrumentid == instrument_id).one()

@patch('pds_pipelines.UPCprocess.getISISid', return_value = 'ISISSERIAL')
@patch('pds_pipelines.UPCprocess.getPDSid', return_value = 'PRODUCTID')
def test_datafiles_insert(mocked_pds_id, mocked_isis_id, session, session_maker, pds_label):
    pds_label = PVLModule({'^IMAGE': ('5600R.IMG', 12),
                           'SPACECRAFT_NAME': 'TEST CRAFT',
                           'INSTRUMENT_NAME': 'TEST INSTRUMENT',
                           'TARGET_NAME': 'TEST TARGET'})
    input_cube = '/Path/to/my/cube.cub'
    upc_id = create_datafiles_record(pds_label, '/Path/to/label/location/label.lbl', '/Path/to/my/cube.cub', session_maker)
    mocked_isis_id.assert_called_with(input_cube)
    mocked_pds_id.assert_called_with(input_cube)

    resp = session.query(model.DataFiles).filter(model.DataFiles.isisid=='ISISSERIAL').first()
    assert upc_id == resp.upcid

def extract_keyword(key):
    if key == 'GisFootprint':
        return Polygon([(153.80256122853893, -32.68515128444211), (153.80256122853893, -33.18515128444211), (153.30256122853893, -33.18515128444211), (153.30256122853893, -32.68515128444211), (153.80256122853893, -32.68515128444211)]).wkt
    return cam_info_dict[key]
@patch('pds_pipelines.UPCkeywords.UPCkeywords.__init__', return_value = None)
@patch('pds_pipelines.UPCkeywords.UPCkeywords.getKeyword', side_effect = extract_keyword)
@patch('pds_pipelines.UPCprocess.getPDSid', return_value = 'PRODUCTID')
def test_search_terms_insert(mocked_product_id, mocked_keyword, mocked_init, session, session_maker, pds_label):
    upc_id = cam_info_dict['upcid']

    model.DataFiles.create(session, **{'upcid': upc_id})

    create_search_terms_record(pds_label, upc_id, '/Path/to/my/cube.cub', session_maker)
    resp = session.query(SearchTerms).filter(SearchTerms.upcid == upc_id).first()

    for key in cam_info_dict.keys():
        resp_attribute = resp.__getattribute__(key)
        if isinstance(resp_attribute, datetime.date):
            resp_attribute = resp_attribute.strftime("%Y-%m-%d %H:%M")
        if isinstance(resp_attribute, WKBElement):
            resp_attribute = str(to_shape(resp_attribute))
        if isinstance(resp_attribute, float):
            np.testing.assert_almost_equal(resp_attribute, cam_info_dict[key], 12)
            continue
        assert cam_info_dict[key] == resp_attribute

@patch('pds_pipelines.UPCkeywords.UPCkeywords.__init__', return_value = None)
def test_json_keywords_insert(mocked_init, session, session_maker, pds_label):
    upc_id = cam_info_dict['upcid']

    model.DataFiles.create(session, **{'upcid': upc_id})

    with patch('pds_pipelines.UPCkeywords.UPCkeywords.label', new_callable=PropertyMock) as mocked_label:
        mocked_label.return_value = pds_label
        create_json_keywords_record(pds_label, upc_id, '/Path/to/my/cube.cub', 'No Failures', session_maker)

    resp = session.query(JsonKeywords).filter(JsonKeywords.upcid == upc_id).first()
    res_json = resp.jsonkeywords
    assert res_json['^IMAGE'][0] == pds_label['^IMAGE'][0]
    assert res_json['TARGET_NAME'] == pds_label['TARGET_NAME']
    assert res_json['INSTRUMENT_NAME'] == pds_label['INSTRUMENT_NAME']
    assert res_json['SPACECRAFT_NAME'] == pds_label['SPACECRAFT_NAME']

def test_generate_isis_processes():
    archive = "galileo_ssi_edr"
    RQ_main = RedisQueue('UPC_ReadyQueue')
    RQ_error = RedisQueue('UPC_ErrorQueue')
    RQ_main.QueueAdd(("./pds_pipelines/tests/data/5600r.lbl", "1", archive))
    context = {'job_id': 1, 'array_id':1, 'inputfile': ''}

    logger = logging.getLogger('UPC_Process')

    # TODO Factor using the Recipe object out of this test
    recipeOBJ = Recipe()
    recipeOBJ.addMissionJson(archive, 'upc')

    # get a file from the queue
    item = literal_eval(RQ_main.QueueGet())
    inputfile = item[0]
    fid = item[1]
    archive = item[2]

    processes, inputfile, caminfoOUT, pwd = generate_isis_processes(inputfile, archive, RQ_error, logger, context)

    original_recipe = recipeOBJ.getRecipe()

    for i, process in enumerate(processes):
        for k, v in process.getProcess().items():
            assert original_recipe[i][k].keys() == v.keys()

# def test_process_isis():
#     RQ_main = RedisQueue('UPC_ReadyQueue')
#     RQ_error = RedisQueue('UPC_ErrorQueue')
#     RQ_main.QueueAdd(("./pds_pipelines/tests/data/5600r.lbl", "1", "galileo_ssi_edr"))
#     context = {'job_id': 1, 'array_id':1, 'inputfile': ''}
#
#     logger = logging.getLogger('UPC_Process')
#
#     failing_command = process_isis(processes)
#     print(inputfile, caminfoOUT, edr_source, failing_command)
#     assert False
