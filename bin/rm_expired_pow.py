import os
import time
import sys
import shutil

def removeOld(path, n_days=14):
    now = time.time()
    cutoff = now - ( n_days * 86400)
    if not os.path.isdir(path):
        print("Unable to access {}".format(path))
        return 1
    for f in os.listdir(path):
        f = os.path.join(path, f)
        if os.stat(f).st_mtime < cutoff:
            try:
                if os.path.isfile(f):
                    #print('rmfile: {}'.format(f))
                    os.remove(f)
                else:
                    #print('rmdir: {}'.format(f))
                    shutil.rmtree(f)
            except OSError as e:
                print(e)
                pass
    return 0

def main():
    """
    try:
        path = sys.argv[1]
    except IndexError:
        print("Usage rm_expired_pow.py <directory> [nDays]")
        return 1
    """
    paths = ['/pds_san/PDS_Services/MAP2/','/pds_san/PDS_Services/POW/']

    try:
        n_days = sys.argv[2]
    except IndexError:
        n_days = 14

    for path in paths:
        removeOld(path, n_days)



if __name__ == "__main__":
    exit(main())
