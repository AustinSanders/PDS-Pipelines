from pds_pipelines.process import Process

import json

import pytest
from unittest.mock import patch, PropertyMock, Mock
from unittest import mock

@pytest.fixture
def empty_process_obj():
    process = Process()
    return process

@pytest.fixture
def process_obj():
    process = Process()
    json_dict = {'data2isis': {'from_':'some_image.img', 'to': 'some_image.cub'},
                 'spiceinit': {'from_':'some_image.cub'}}
    json_str = json.dumps(json_dict)
    process.JSON2Process(json_str)
    return process

@pytest.fixture
def json_dict():
    json_dict = {'data2isis': {'from_':'some_image.img', 'to': 'some_image.cub'},
                 'spiceinit': {'from_':'some_image.cub'}}
    return json_dict

def test_default_process(empty_process_obj):
    assert len(empty_process_obj.getProcess().keys()) == 1
    assert empty_process_obj.getProcess()[''] == None
    assert empty_process_obj.getProcessName() == ''

def test_process_2_json(process_obj, json_dict):
    process_str = process_obj.Process2JSON()
    assert process_str == json.dumps(json_dict)

# This seems "wrong"
def test_json_2_process(empty_process_obj, json_dict):
    json_str = json.dumps(json_dict)
    empty_process_obj.JSON2Process(json_str)
    assert empty_process_obj.getProcess().keys() == json_dict.keys()
    assert empty_process_obj.getProcessName() == 'spiceinit'

def test_set_process(process_obj):
    assert process_obj.getProcessName() == 'spiceinit'
    process_obj.setProcess('Banana')
    assert process_obj.getProcessName() == 'Banana'
    assert len(process_obj.getProcess().keys()) == 2

# Reduces the number of processes in the process object from 2 to 1
# Also seems "wrong"
def test_change_process(process_obj):
    assert process_obj.getProcessName() == 'spiceinit'
    assert len(process_obj.getProcess()) == 2
    process_obj.ChangeProcess('Banana')
    assert len(process_obj.getProcess()) == 1
    assert process_obj.getProcessName() == 'Banana'
    assert list(process_obj.getProcess().keys()) == ['Banana']
    assert list(process_obj.getProcess()['Banana'].keys()) == ['from_']

# Reduces the number of processes from the json dict from 2 to 1
# Also seems "wrong"
def test_process_from_recipe(empty_process_obj, json_dict):
    recipe = [{i: json_dict[i]} for i in json_dict]
    empty_process_obj.ProcessFromRecipe('spiceinit', recipe)
    assert empty_process_obj.getProcessName() == 'spiceinit'
    assert list(empty_process_obj.getProcess().keys()) == ['spiceinit']

def test_update_parameter(process_obj):
    assert process_obj.getProcess()['data2isis']['from_'] == 'some_image.img'
    assert process_obj.getProcess()['spiceinit']['from_'] == 'some_image.cub'
    process_obj.updateParameter('from_', 'some_other_image.cub')
    assert process_obj.getProcess()['data2isis']['from_'] == 'some_image.img'
    assert process_obj.getProcess()['spiceinit']['from_'] == 'some_other_image.cub'

def test_new_process(process_obj):
    assert process_obj.getProcessName() == 'spiceinit'
    process = 'Banana'
    process_obj.newProcess(process)
    assert process_obj.getProcessName() == process
    assert len(process_obj.getProcess()[process].keys()) == 0

def test_add_parameter(process_obj):
    assert len(process_obj.getProcess()['spiceinit'].keys()) == 1
    process_obj.AddParameter('ckrecon', True)
    assert len(process_obj.getProcess()['spiceinit'].keys()) == 2

def test_gdal_ibit(process_obj):
    gdal_bit_type = process_obj.GDAL_OBit('unsignedbyte')
    assert gdal_bit_type == 'Byte'

@pytest.mark.xfail(raises=Exception)
def test_gdal_ibit_fail(process_obj):
    process_obj.GDAL_OBit('apple')

def test_gdal_creation(process_obj):
    gdal_bit_type = process_obj.GDAL_Creation('JPEG')
    assert gdal_bit_type == 'quality=100'

@pytest.mark.xfail(raises=Exception)
def test_gdal_creation_fail(process_obj):
    process_obj.GDAL_Creation('apple')
