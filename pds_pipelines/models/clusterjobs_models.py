from sqlalchemy.ext.declarative import declarative_base, declared_attr
from sqlalchemy import (Column, Integer, Float,
                        Time, String, Boolean, PrimaryKeyConstraint, ForeignKey)

Base = declarative_base()

class Processing(Base):
    _tablename_ = 'processing'
    processingid = Column(Integer, primary_key=True, autoincrement=True)
    customerid = Column(Integer, ForeignKey("customers.customerid"))
    submitted = Column(Time)
    queued = Column(Time)
    started = Column(Time)
    finished = Column(Time)
    accessed = Column(Time)
    typeid = Column(Integer, ForeignKey("processtypes.typeid"))
    save = Column(Time)
    xml = Column(String)
    error = Column(String)
    key = Column(String(64))
    notified = Column(Time)
    title = Column(String(256))
    description = Column(String)
    purged = Column(Time)
    template = Column(Boolean)


class ProcessTypes(Base):
    _tablename_ = 'processtypes'
    typeid = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(64))


class Customers(Base):
    _tablename_ = 'customers'
    customerid = Column(Integer, primary_key=True, autoincrement=True)
    email = Column(String(64))
    name = Column(String(128))
    affiliation = Column(String(64))
    username = Column(String(16))
    status = Column(String(1))
