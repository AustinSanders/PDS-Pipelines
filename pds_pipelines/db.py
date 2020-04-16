from sqlalchemy import create_engine
from pds_pipelines.config import credentials as c
from sqlalchemy import MetaData, Table
from sqlalchemy.orm import mapper, sessionmaker

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
        engine = create_engine('postgresql://{}:{}@{}:{}/{}'.format(c[cred]['user'],
                                                                    c[cred]['pass'],
                                                                    c[cred]['host'],
                                                                    c[cred]['port'],
                                                                    c[cred]['db']))
    except KeyError:
        print("Credentials not found for {}".format(cred))
    metadata = MetaData(bind=engine)
    session_maker = sessionmaker(bind=engine)
    return session_maker, engine
