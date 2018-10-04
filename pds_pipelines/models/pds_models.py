from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.types import TIMESTAMP
from sqlalchemy import (Column, Integer, Float, String, Boolean,
                        PrimaryKeyConstraint)

Base = declarative_base()

class ProcessRuns(Base):
    _tablename_ = 'process_runs'
    processid = Column(Integer, primary_key=True)
    fileid = Column(Integer)
    process_date = Column(TIMESTAMP)
    process_typeid = Column(Integer)
    process_out = Column(Boolean)


class Files(Base):
    _tablename_ = 'files'
    fileid = Column(Integer, primary_key=True)
    # @TODO set as foreign key for archive
    archiveid = Column(Integer)
    filename = Column(String(2048))
    entry_date = Column(TIMESTAMP)
    checksum = Column(String(35))
    upc_required = Column(Boolean)
    validation_required = Column(Boolean)
    header_only= Column(Boolean)
    release_date = Column(TIMESTAMP)
    file_url = Column(String(2048))
    isis_errors = Column(Boolean, default=False)
    file_size = Column(Integer)
    di_pass = Column(Boolean)
    di_date = Column(TIMESTAMP)


class Archives(Base):
    _tablename_ = 'archives'
    # @TODO auto increment
    archiveid = Column(Integer, primary_key=True)
    archive_name = Column(String(1024))
    missionid = Column(Integer)
    pds_archive = Column(Boolean)
    primary_node = Column(String(64))
