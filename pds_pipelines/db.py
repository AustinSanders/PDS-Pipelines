from sqlalchemy import create_engine, inspect
from pds_pipelines.config import credentials as c
from sqlalchemy import MetaData, Table
from sqlalchemy.orm import mapper, sessionmaker
from sqlalchemy.exc import OperationalError

def db_connect(cred):
    """
    Given some crediential string that is present as a key in the config
    credentials dictionary, generate the engine and associated session_maker
    objects for a database.

    Parameters
    ----------
    cred : str
           Credential string lookup from the config

    Returns
    -------
    session_maker : Object
                    sqlalchemy session maker object that can generate
                    session connections to the database
    engine : Object
             sqlalchemy engine object for the session maker
    """
    session_maker = None
    engine = None
    try:
        database_url = 'postgresql://{}:{}@{}:{}'.format(c[cred]['user'],
                                                            c[cred]['pass'],
                                                            c[cred]['host'],
                                                            c[cred]['port'])
    except KeyError:
        print("Credentials not found for {}".format(cred))

    try:
        engine = create_engine(database_url)
        insp = inspect(engine)
        db_list = insp.get_schema_names()
        engine.url.database = c[cred]['db']
        if c[cred]['db'] in db_list:
            engine.connect()
    except OperationalError:
        print(f"WARNING:  Unable to create a database connection for credential specification '{cred}'")
        return None, None
    metadata = MetaData(bind=engine)
    session_maker = sessionmaker(bind=engine)
    return session_maker, engine
