from sqlalchemy.ext.declarative import declarative_base, declared_attr
from sqlalchemy import (Column, Integer, Float,
                        Time, String, Boolean, PrimaryKeyConstraint, ForeignKey, CHAR)
from sqlalchemy.dialects.postgresql import JSONB
from geoalchemy2 import Geometry

import datetime

Base = declarative_base()


def create_table(keytype, *args, **kwargs):
    """ Returns a table based on the keytype description.

    Parameters
    ----------
    keytype : str
        A type description of the table's value.

    Returns
    -------
    out : :class:`.Instruments or :class:`.DataFiles or :class:`.Targets
        A SQLAlchemy table with type specific column specification.
    """

    try:
        return class_map[keytype.lower()](**kwargs)
    except NameError:
        print('Keytype {} not found\n'.format(keytype))


class DataFiles(Base):
    __tablename__ = 'datafiles'
    upcid = Column(Integer, primary_key=True, autoincrement = True)
    isisid = Column(String(256))
    productid = Column(String(256))
    source = Column(String(1024))
    detached_label = Column(String(1024))
    instrumentid = Column(Integer, ForeignKey("instruments.instrumentid"))
    targetid = Column(Integer, ForeignKey("targets.targetid"))
    level = Column(CHAR(1))


class Instruments(Base):
    __tablename__ = 'instruments'
    instrumentid = Column(Integer, primary_key=True, autoincrement = True)
    instrument = Column(String(256), nullable=False)
    displayname = Column(String(256))
    mission = Column(String(256))
    spacecraft = Column(String(256))
    description = Column(String(256))
    #product_type = Column(String(8))


class Targets(Base):
    __tablename__ = 'targets'
    targetid = Column(Integer, primary_key = True, autoincrement = True)
    naifid = Column(Integer)
    targetname = Column(String(20), nullable = False)
    system = Column(String(20), nullable = False)
    displayname = Column(String(20))
    aaxisradius = Column(Float)
    baxisradius = Column(Float)
    caxisradius = Column(Float)
    description = Column(String(1024))
    #iau_mean_radius = Column(Float)


class NewStats(Base):
    __tablename__ = 'new_stats'
    instrumentid = Column(Integer, primary_key=True)
    targetid = Column(Integer, primary_key=True)
    targetname = Column(String(256))
    system = Column(String(20))
    instrument = Column(String(256))
    mission = Column(String(256))
    spacecraft = Column(String(256))
    displayname = Column(String(256))
    start_date = Column(Time)
    stop_date = Column(Time)
    last_published = Column(Time)
    bands = Column(Integer)
    total = Column(Integer)
    errors = Column(Integer)


class SearchTerms(Base):
    __tablename__ = 'search_terms'
    upcid = Column(Integer, primary_key=True)
    upctime = Column(Time)
    starttime = Column(Time)
    solarlongitude = Column(Float)
    meangroundresolution = Column(Float)
    minimumemission = Column(Float)
    maximumemission = Column(Float)
    emissionangle = Column(Float)
    minimumincidence = Column(Float)
    maximumincidence = Column(Float)
    incidenceangle = Column(Float)
    minimumphase = Column(Float)
    maximumphase = Column(Float)
    phaseangle = Column(Float)
    targetid = Column(Integer, ForeignKey("targets.targetid"))
    instrumentid = Column(Integer, ForeignKey("instruments.instrumentid"))
    missionid = Column(Integer)
    pdsproductid = Column(Integer)
    err_flag = Column(Boolean)
    isisfootprint = Column(Geometry())


class JsonKeywords(Base):
    __tablename__ = "json_keywords"
    upcid = Column(Integer, primary_key=True)
    jsonkeywords = Column(JSONB)
    

class_map = {
    'datafiles': DataFiles,
    'instruments': Instruments,
    'targets' : Targets
}
