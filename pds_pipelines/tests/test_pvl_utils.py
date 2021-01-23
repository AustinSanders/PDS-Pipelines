import os
import pvl

import pytest
import tempfile
from unittest.mock import patch, PropertyMock, Mock
from unittest import mock

from pds_pipelines.pvl_utils import find_keyword, lower_keys, load_pvl

@pytest.mark.parametrize("object, key, group, expected",
                        [(pvl.PVLModule({"Banana": {"Apple": "Fruit"}}),'Apple', None, 'Fruit'),
                         (pvl.PVLModule({"Banana": {"Apple": "Fruit"}}),'Orange', None, None),
                         (pvl.PVLModule({"Banana": {"Apple": "Fruit"}}),'Apple', 'Banana', 'Fruit')])
def test_find_keyword(expected, group, key, object):
    output_val = find_keyword(object, key, group)
    assert output_val == expected

def test_lower_keys():
    output = lower_keys([{"FRUIT": "APPLE",}, {"FRUIT": "BANANA",}])
    for i in output:
        for key in i:
            assert key == "fruit"

def test_load_pvl():
    with tempfile.TemporaryDirectory() as tempdir:
        pvl_str = '''
Group = Banana
    FRUIT = APPLE
End_Group
        '''
        temp_pvl_file = os.path.join(tempdir, 'tempPvl.pvl')
        with open(temp_pvl_file, 'w') as fp:
            fp.write(pvl_str)

        output_label = load_pvl(temp_pvl_file)
        assert list(output_label.keys()) == ["banana"]
        assert list(output_label["banana"].keys()) == ["fruit"]

def test_load_pvl_replace():
    with tempfile.TemporaryDirectory() as tempdir:
        pvl_str = '''
Group = Banana
    FRUIT = APPLE&PIE
End_Group
        '''
        temp_pvl_file = os.path.join(tempdir, 'tempPvl.pvl')
        with open(temp_pvl_file, 'w') as fp:
            fp.write(pvl_str)

        output_label = load_pvl(temp_pvl_file)
        assert list(output_label.keys()) == ["banana"]
        assert list(output_label["banana"].keys()) == ["fruit"]

def test_load_pvl_module():
    input_pvl = pvl.PVLModule({"Banana": {"Fruit": "Apple"}})
    output_label = load_pvl(input_pvl)
    assert list(output_label.keys()) == ["banana"]
    assert list(output_label["banana"].keys()) == ["fruit"]
