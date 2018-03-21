from sqlalchemy.ext.declarative import declarative_base, DeferredReflection
from datetime import time
from functools import singledispatch, update_wrapper
from sqlalchemy import Table, MetaData

Base = declarative_base(DeferredReflection)


# @TODO find a way to use metadata without manual setup
metadata = None
def set_meta(meta):
    metadata = meta

def customdispatch(func):
    """ Dispatch on obj, not type(obj)
    
    Decorator with functionality similar to singledispatch,
    but dispatches based 'arg[0]' rather than 'type(arg[0])'.
    This is necessary when the first object is specified as
    a type, not an instance of a type.
    """
    dispatcher = singledispatch(func)
    def wrapper(*args, **kw):
        """ Custom wrapper returns first arg.  Must of of type 'type'"""
        return dispatcher.dispatch(args[0])(*args, **kw)
    wrapper.register = dispatcher.register
    update_wrapper(wrapper, func)
    return wrapper   

def meta(keytype, **kwargs):
    return _meta(_type(keytype), kwargs)

@customdispatch
def _meta(c_type, **kwargs):
    """ If type not listed in single dispatch..."""
    raise(NotImplementedError)

@_meta.register(str)
def _(c_type, **kwargs):
    return meta_string(kwargs)

@_meta.register(float)
def _(c_type, **kwargs):
    return meta_precision(kwargs)

@_meta.register(int)
def _(c_type, **kwargs):
    return meta_integer(kwargs)

@_meta.register(time)
def _(c_type, **kwargs):
    return meta_time(kwargs)

@_meta.register(bool)
def _(c_type, **kwargs):
    return meta_boolean(kwargs)

def _type(self, s_type):
    """ Converts a string type description to a concrete type.
    """
    if s_type == 'string':
        return str
    elif s_type == 'double':
        return float
    elif s_type == 'integer':
        return int
    elif s_type == 'time':
        return time
    elif s_type == 'boolean':
        return bool



# @TODO Replace autoloaded table declarations with expanded forms
#   - We'll have to look at database / tables and make them by hand
class datafiles(Base):
    __table__ = Table('datafiles', metadata, autoload=True)

class instruments(Base):
    __table__ = Table('instruments_meta', metadata, autoload=True)

class targets(Base):
    __table__ = Table('targets_meta', metadata, autoload=True)

class keywords(Base):
    __table__ = Table('keywords', metadata, autoload=True)

class meta_precision(Base):
    __tablename__ = 'meta_precision'
    upcid = Column(Integer, primary_key = True)
    typeid = Column(Integer)
    # @TODO
    value = Column()

class meta_time(Base):
    ___tablename__ = 'meta_time'
    upcid = Column(Integer, primary_key = True)
    typeid = Column(Integer)
    # @TODO
    value = Column()

class meta_string(Base):
    __tablename__ = 'meta_string'
    upcid = Column(Integer, primary_key = True)
    typeid = Column(Integer)
    # @TODO
    value = Column()

class meta_integer(Base):
    __tablename__ = 'meta_integer'
    upcid = Column(Integer, primary_key = True)
    typeid = Column(Integer)
    # @TODO
    value = Column()

class meta_boolean(Base):
    __tablename__ = 'meta_boolean'
    upcid = Column(Integer, primary_key = True)
    typeid = Column(Integer)
    # @TODO
    value = Column()

class meta_geometry(Base):
    __tablename__ = 'meta_geometry'
    upcid = Column(Integer, primary_key=True)
    typeid = Column(Integer)
    value = Column(Geometry('geometry'))


if __name__ == "__main__":
    from db import db_connect
    _,_,_,engine = db_connect('upcdev')
    
    set_meta(MetaData(bind=engine))

    m_keytype = 'boolean'
    db_input = meta(m_keytype, engine, metadata, upcid = 1, typeid=m_keytype, value = 42)
    attrs = vars(db_input)
    print(', '.join("%s: %s" % item for item in attrs.items()))
