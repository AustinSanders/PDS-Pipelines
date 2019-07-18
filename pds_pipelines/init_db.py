import inspect
from pds_pipelines.db import db_connect
from pds_pipelines.config import pds_db, upc_db
from sqlalchemy import MetaData
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.exc import OperationalError, ProgrammingError
import pds_pipelines.models.pds_models as pds_models
import pds_pipelines.models.upc_models as upc_models


def create_all(engine, module):
    Base = declarative_base()
    # Get tables already in database
    m = MetaData()
    m.reflect(engine)

    # Grab all the classes defined in a module.  Used with filter, this ensures that we're grabbing only tables.
    tables = [m[1] for m in inspect.getmembers(module, inspect.isclass) if m[1].__module__ == module.__name__]
    filter(lambda x: isinstance(x, Base), tables)

    # Throw out any tables that already exist.  Necessary because "ProgrammingError" exception is unable to
    #  differentiate between failure conditions w.r.t. table creation
    l = [x for x in tables if x.__tablename__ not in m.tables]

    # If a change was made, then we haven't setted.  Necessary to track due to the existence of foreign keys.
    changed = 1
    temp = []

    while l and changed:
        changed = 0
        for table in l:
            try:
                table.__table__.create(engine)
                changed = 1
            except ProgrammingError as e:
                # If we can't create a table, it may simply be due to the fact that we simply haven't created
                #  a table upon which it is dependent.
                temp.append(table)

        # If nothing changed and we still have things that weren't created, then something is (probably) wrong with the schema.
        if temp and not changed:
            print("Unable to resolve schema.  Check foreign keys for the following tables: {}".format(temp))
            exit(1)
        l = temp
        temp = []


def main():
    _, pds_engine = db_connect(pds_db)
    _, upc_engine = db_connect(upc_db)

    create_all(pds_engine, pds_models)
    create_all(upc_engine, upc_models)


if __name__ == "__main__":
    main()
