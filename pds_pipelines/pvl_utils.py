import os
import sys
import pvl
import re

def find_keyword(obj, key, group=None):
    if group is not None:
        return find_keyword(obj[group], key)
    if key is None or obj is None:
        return None
    elif key in obj:
        return obj[key]
    for k, v in obj.items():
        if isinstance(v, dict):
            F_item = find_keyword(v, key)
            if F_item is not None:
                return F_item

def lower_keys(x):
    if isinstance(x, list):
        return [lower_keys(v) for v in x]
    elif isinstance(x, dict):
        return dict((k.lower(), lower_keys(v)) for k, v in x.items())
    else:
        return x

def load_pvl(input_pvl, decoder=None):
    if isinstance(input_pvl, str):
        try:
            label = pvl.load(input_pvl, decoder=decoder)
        except:
            # Some labels are poorly formatted or include characters that
            # cannot be parsed with the PVL library.  This finds those
            # characters and replaces them so that we can properly parse
            # the PVL.
            with open(input_pvl, 'r') as f:
                filedata = f.read()

            filedata = filedata.replace(';', '-').replace('&', '-')
            filedata = re.sub(r'\-\s+', r'', filedata, flags=re.M)

            with open(input_pvl, 'w') as f:
                f.write(filedata)

            label = pvl.load(input_pvl)

        label = lower_keys(label)

    elif isinstance(input_pvl, pvl.PVLModule):
        label = lower_keys(input_pvl)

    return label

class PVLDecoderNoScientificNotation(pvl.decoder.PVLDecoder):
    @staticmethod
    def decode_decimal(value: str):
        """Returns a Python ``int`` or ``float`` as appropriate
        based on *value*.  Raises a ValueError otherwise.
        """
        # Check for scientific notation
        pattern = re.compile("^([0-9]+)[Ee]([0-9]+)$")
        if pattern.match(str(value)):
            return str(value)

        try:
            return int(value, base=10)
        except ValueError:
            return float(value)
