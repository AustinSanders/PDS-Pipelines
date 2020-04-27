#!/usr/bin/env python
import sys
import argparse

from pds_pipelines.db import db_connect
from pds_pipelines.models.upc_models import DataFiles
from pds_pipelines.models.pds_models import Files
from pds_pipelines.config import credentials, upc_db, pds_db

def parse_args():
    parser = argparse.ArgumentParser(description='Database Record Delete')
    parser.add_argument('--pdsid', '-r', dest='pdsid', help="PDS id to delete from " +
                        "the selected databases", type=str)
    parser.add_argument('-di', dest="di", help="Flag defining if the record should " +
                        "be deleted from the di database", action='store_true')
    parser.add_argument('-upc', dest="upc", help="Flag defining if the record should " +
                        "be deleted from the upc database", action='store_true')
    parser.set_defaults(di=False)
    parser.set_defaults(upc=False)
    args = parser.parse_args()
    return args

def main(user_args):
    pds_id = user_args.pdsid
    delete_from_di = user_args.di
    delete_from_upc = user_args.upc

    if delete_from_di:
        pds_session_maker, pds_engine = db_connect(pds_db)
        pds_session = pds_session_maker()

        query_res = pds_session.query(Files).filter(
                    Files.filename.contains(pds_id))
        num_pds_queries = len(list(query_res))

        while(True):
            print(f'You will be deleteing {num_pds_queries} from the di ' +
                  f"database {credentials[pds_db]['db']}")
            user_answer = input('Are you sure?[Y/N]:')

            if user_answer == 'Y' or user_answer == 'N':
                break
            else:
                print(f'Invalid input: {user_answer}')

        if user_answer == 'Y':
            for record in query_res:
                pds_session.delete(record)
        pds_session.commit()
        pds_session.flush()
        pds_session.close()

    if delete_from_upc:
        upc_session_maker, upc_engine = db_connect(upc_db)
        upc_session = upc_session_maker()

        query_res = upc_session.query(DataFiles).filter(
            DataFiles.productid == pds_id)
        num_upc_queries = len(list(query_res))
        while(True):
            print(f'You will be deleteing {num_upc_queries} from the upc ' +
                  f"database {credentials[upc_db]['db']}")
            user_answer = input('Are you sure?[Y/N]:')

            if user_answer =='Y' or user_answer == 'N':
                break
            else:
                print(f'Invalid input: {user_answer}')

        if user_answer == 'Y':
            for record in query_res:
                upc_session.delete(record)
        upc_session.commit()
        upc_session.flush()
        upc_session.close()

if __name__ == "__main__":
    sys.exit(main(parse_args()))
