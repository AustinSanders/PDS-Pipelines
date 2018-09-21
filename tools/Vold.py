import os
import pvl
import glob
import json
import argparse

def load_pvl(pvl_file_path):
    with open(pvl_file_path, 'r') as f:
        f.readline()
        data = f.read()
    voldesc = pvl.loads(data)
    return voldesc

class Args:
    def __init__(self):
        pass

    def parse_args(self):
        parser = argparse.ArgumentParser()
        parser.add_argument('--textfile','-f',help='Input text file path .txt')
        parser.add_argument('--output','-o',help='Output file path .json')
        args = parser.parse_args()
        self.textfile = args.textfile
        self.output = args.output

def main():
    args = Args()
    args.parse_args()
    
    if args.texfile == str:
        filePath = open(args.textfile,'r')
        lines = filePath.readlines()
        length = len(lines)
        vol_val = {}
        for n in range(length):
            voldesc = load_pvl(lines[n].rstrip())
            dataset_id = voldesc['VOLUME']['DATA_SET_ID']
            volume_name = voldesc['VOLUME']['VOLUME_NAME']
            if isinstance(dataset_id, (list, tuple, set)):
                vol_val[volume_name] = len(dataset_id)
            else:
                vol_val[volume_name]= 1
    else:
        filePath = open(args.singlefile,'r')
        lines = filePath.readlines()
        length = len(lines)
        vol_val = {}
        for n in range(length):
            voldesc = load_pvl(lines[n].rstrip())
            dataset_id = voldesc['VOLUME']['DATA_SET_ID']
            volume_name = voldesc['VOLUME']['VOLUME_NAME']
            if isinstance(dataset_id, (list, tuple, set)):
                vol_val[volume_name] = len(dataset_id)
            else:
                vol_val[volume_name]= 1

    
    if type(args.output) == str:
        f = open(args.output,'w')
        f.write(str(json.dumps(vol_val)))
    else:
        print(json.dumps(vol_val))

if __name__ == '__main__':
    main()