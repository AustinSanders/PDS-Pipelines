import datetime
import pytz
import json
from sqlalchemy import MetaData, Table
from sqlalchemy.orm import mapper, sessionmaker
from FindDI_Ready import archive_expired, volume_expired
from RedisQueue import RedisQueue
from db import db_connect
# from models import upc_models, pds_models


def main():
    PDS_info = json.load(open('/usgs/cdev/PDS/bin/PDSinfo.json', 'r'))
    reddis_queue = RedisQueue('UPC_ReadyQueue')

    try:
        # Safe to use prd database here because there are no writes/edits.
        session, files, archives, engine = db_connect('pdsdi')
        metadata = MetaData(bind=engine)
        files = Table('files', metadata, autoload=True)
        archives = Table('archives', metadata, autoload=True)

        # TODO use db models in pds_models.py
        #  Current problem is that they're considered 'already mapped'
        #  DeferredReflection base class might be part of the answer.
        class Files(object):
            pass

        class Archives(object):
            pass

        mapper(Files, files)
        mapper(Archives, archives)
        Session = sessionmaker()
        session = Session()

    # @TODO Catch exceptions by type.  Bad practice to 'except Exception,' but
    #   I don't know what exception could happen here.
    except Exception as e:
        print(e)
        return 1

    # For each archive in the db, test if there are files that are ready to
    #  process
    for archive_id in session.query(Files.archiveid).distinct():
        result = session.query(Files).filter(files.c.archiveid == archive_id,
                                             files.c.upc_required == 't')

        # Get filepath from archive id
        archive_name = session.query(Archives.archive_name).filter(
            archives.c.archiveid == archive_id).first()

        # No archive name = no path.  Skip these values.
        if (archive_name is None):
            continue

        try:
            # Since results are returned as lists, we have to access the 0th
            #  element to pull out the string archive name.
            fpath = PDS_info[archive_name[0]]['path']
        except KeyError:
            continue

        # Add each file in the archive to the redis queue.
        for element in result:
            fName = fpath + element.filename
            reddis_queue.QueueAdd(fName)
    return 0


if __name__ == "__main__":
    main()
