import os
import pvl
import glob
import json
def load_pvl(pvl_file_path):
    with open(pvl_file_path, 'r') as f:
        f.readline()
        data = f.read()
    voldesc = pvl.loads(data)
    return voldesc

def main():
    s = input('File Path --> ')
    filePath = glob.glob(s)
    length = len(filePath)
    vol_val = {}
    print(length)
    for n in range(length):
        voldesc = load_pvl(filePath[n])
        dataset_id = voldesc['VOLUME']['DATA_SET_ID']
        volume_name = voldesc['VOLUME']['VOLUME_NAME']

        if isinstance(dataset_id, (list, tuple, set)):
            vol_val[volume_name] = len(dataset_id)
        else:
            vol_val[volume_name]= 1
            print(json.dumps(vol_val))
if __name__ == '__main__':
    main()