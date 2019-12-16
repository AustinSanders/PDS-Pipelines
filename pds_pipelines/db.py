from sqlalchemy import create_engine
from pds_pipelines.config import credentials as c
from sqlalchemy import MetaData, Table
from sqlalchemy.orm import mapper, sessionmaker

def db_connect(cred):
    """
    Parameters
    ----------
    cred : str

    Returns
    -------
    session
    engine
    """
    Session = None
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
    Session = sessionmaker(bind=engine)
    return Session, metadata
