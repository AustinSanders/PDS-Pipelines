from sqlalchemy import create_engine
from config import credentials as c
from sqlalchemy import MetaData, Table
from sqlalchemy.orm import mapper, sessionmaker


class Files(object):
    pass


class Archives(object):
    pass


def db_connect(cred):
    try:
        engine = create_engine('postgresql://{}:{}@{}:{}/{}'.format(c[cred]['user'],
                                                                    c[cred]['pass'],
                                                                    c[cred]['host'],
                                                                    c[cred]['port'],
                                                                    c[cred]['db']))
    except KeyError:
        print("Credentials not found for {}".format(cred))


    metadata = MetaData(bind=engine)
    files = Table('files', metadata, autoload=True)
    archives = Table('archives', metadata, autoload=True)
    filesmapper = mapper(Files, files)
    archivesmapper = mapper(Archives, archives)
    Session = sessionmaker()
    session = Session()

    return session, files, archives, engine
