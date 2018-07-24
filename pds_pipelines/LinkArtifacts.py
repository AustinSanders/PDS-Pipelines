import os
import pvl
from xmljson import badgerfish as bf
from xml.etree.ElementTree import fromstring, ParseError
from pds_pipelines.config import xml_root
from pds_pipelines.RedisQueue import RedisQueue
from ast import literal_eval

def main():
    RQ = RedisQueue('LinkQueue')
    while int(RQ.QueueSize()) > 0:
        # Grab a tuple of values from the redis queue
        item = literal_eval(RQ.QueueGet().decode('utf-8'))
        # Split tuple into two values
        inputfile = item[0]
        archive = item[1]

        xml_file_path = xml_root + archive + '/config/config.xml'
        try:
            xml = parse_xml(xml_file_path)
        except(ParseError):
            continue
        link_src_path = xml['instrument']['path']['file']['$']
        link_dest_path = xml['instrument']['path']['datalink']['$']

        voldesc = load_pvl(inputfile)
        dataset_id = format_id(voldesc['VOLUME']['DATA_SET_ID'])
        volume_id = format_id(voldesc['VOLUME']['VOLUME_ID'])

        src = os.path.join(link_src_path, volume_id)
        dest = os.path.join(link_dest_path, dataset_id, volume_id)
        # Split the path into (/tuple/of/, /paths) 
        link_path = os.path.split(dest)
        # Create intermediate directories if they don't exist
        os.makedirs(link_path[0], exist_ok=True)
        os.symlink(src, dest)


def format_id(ds_id):
    # Remove all quotes, braces, brackets, parentheses, commas, spaces
    formatted_id = ''.join(c for c in ds_id if c not in '\'\"{}[](), ')
    formatted_id = formatted_id.replace('/','_')
    formatted_id = formatted_id.lower()
    return formatted_id


def parse_xml(xml_file_path):
    with open(xml_file_path) as f:
        data = f.read()
        xml = bf.data(fromstring(data))
    return xml


def load_pvl(pvl_file_path):
    with open(pvl_file_path, 'r') as f:
        f.readline()
        data = f.read()
    voldesc = pvl.loads(data, strict=False)
    return voldesc


if __name__ == '__main__':
    main()
