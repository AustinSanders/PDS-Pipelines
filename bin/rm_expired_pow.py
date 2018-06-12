import os
from datetime import datetime, timedelta
import sys
import shutil

from models import clusterjobs_models
from db import db_connect
from sqlalchemy import and_

def remove(paths, keys, id2task, session):
    """ Removes a job from the supplied path based on its key.
    
    Parameters
    ----------
    paths : list[String]
        A list of string paths from which the keys will be deleted
    keys : list[sqlalchemy.collections.result(String, String)]
        Tuple of (typeid, key) where 'typeid' determines the path from
         which the key will be deleted and 'key' is the item to be
         deleted.
        A typeid of 1 = POW , 2 = MAP2
    id2task : dict
        A mapping between taskid and task description
    session : sqlAlchemy.Session
        A session object used to update the 'purged' column related to the
         deleted file
    """
    for item in keys:
        typeid = int(item[0])
        path = paths[id2task[typeid]]
        key = item[1]
        f = os.path.join(path, key)
        """
        try:
            if os.path.isfile(f):
                os.remove(f)
            else:
                shutil.rmtree(f)
            set_purged(session, key)
        except OSError as e:
            print(e)
            pass
        """

def set_purged(session, key):
    now = datetime.now()
    k = session.query(clusterjobs_models.Processing).filter(
        clusterjobs_models.Processing.key == key).update({'purged': now})
    session.commit()


def get_old_keys(session, n_days=14):
    cutoff = datetime.now() - timedelta(days=n_days)
    old = session.query(clusterjobs_models.Processing.typeid, clusterjobs_models.Processing.key).filter(
        and_(clusterjobs_models.Processing.notified < cutoff,
             clusterjobs_models.Processing.purged == None))
    return old


def map_type_ids(session):
    ids = session.query(clusterjobs_models.ProcessTypes.typeid, clusterjobs_models.ProcessTypes.name)
    out = dict(ids)
    return out


def main():
    paths = {'MAP2':'/pds_san/PDS_Services/MAP2/',
             'POW':'/pds_san/PDS_Services/POW/'}

    session, _ = db_connect('clusterjob_prd')

    id2task = map_type_ids(session)

    try:
        n_days = sys.argv[1]
    except IndexError:
        n_days = 14

    old_files = get_old_keys(session)
    remove(paths, old_files, id2task, session)

    return 0


if __name__ == "__main__":
    exit(main())
