from sqlalchemy import create_engine, inspect
from pds_pipelines.config import credentials as c
from sqlalchemy import MetaData, Table
from sqlalchemy.orm import mapper, sessionmaker
import sqlalchemy

def db_connect(cred):
    """
    Given some credential string that is present as a key in the config
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
        database_url = 'postgresql://{}:{}@{}:{}/{}'
        public_database_url = database_url.format(c[cred]['user'],
                                                  c[cred]['pass'],
                                                  c[cred]['host'],
                                                  c[cred]['port'],
                                                  'postgres')

        engine = create_engine(public_database_url, pool_pre_ping=True)
    except KeyError:
        print("Credentials not found for {}".format(cred))

    # Attempt to connect to a default database in the specific database instance
    # If this is not possible, then return None for session and engine. Otherwise
    # create a new URL with the actual database to connect to and move forward
    try:
        engine.connect()
    except sqlalchemy.exc.OperationalError:
        print(f"WARNING:  Unable to create a database connection for credential specification '{cred}'")
        return None, None

    engine = create_engine(database_url.format(c[cred]['user'],
                                               c[cred]['pass'],
                                               c[cred]['host'],
                                               c[cred]['port'],
                                               c[cred]['db']), pool_pre_ping=True)

    metadata = MetaData(bind=engine)
    session_maker = sessionmaker(bind=engine)
    return session_maker, engine
