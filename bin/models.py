from sqlalchemy.ext.declarative import declarative_base, DeferredReflection
from functools import singledispatch, update_wrapper
from sqlalchemy import Table, MetaData, Column, Integer, Time, String, Boolean
from sqlalchemy import Float as Double
from geoalchemy2 import Geometry

Base = declarative_base(DeferredReflection)

def dispatch(func):
    """ Dispatch on obj, not type(obj)
    
    Decorator with functionality similar to singledispatch,
    but dispatches on 'arg[0]' rather than 'type(arg[0])'.
    This is necessary when the first object is specified as
    a type, not an instance of a type.
    """
    dispatcher = singledispatch(func)
    def wrapper(*args, **kw):
        """ Custom wrapper returns first arg.  Must be of type 'type'"""
        return dispatcher.dispatch(args[0])(*args, **kw)
    wrapper.register = dispatcher.register
    update_wrapper(wrapper, func)
    return wrapper   

# @TODO rename - currently named meta because all other classes
#  are prefixed with 'meta' (meta_precision, meta_integer, etc.),
#  but it doesn't make much sense as is.
def meta(keytype, *args, **kwargs):
    """ Returns a table based on the keytype description.
    
    Parameters
    ----------
    keytype : str
        A type description.
    *args
        Variable length argument list.
    **kwargs
        Arbitrary keyword arguments.

    Returns
    -------
    out : :class:`.meta_string` or :class:`.meta_precision` or :class:`.meta_integer` or :class:`.meta_time` or :class:`.meta_boolean`
        A SQLAlchemy table with type specific column specification.
    """
    # Evaluate string type description to yield formal type
    try:
        return _meta(eval(keytype.capitalize()), *args, **kwargs)
    except NameError:
        print('Keytype {} not found\n'.format(keytype))
        return None


@dispatch
def _meta(c_type, *args, **kwargs):
    """ If implementation for specified type doesn't exist..."""
    raise(NotImplementedError)

@_meta.register(String)
def _(c_type, **kwargs):
    return meta_string(**kwargs)

@_meta.register(Double)
def _(c_type, **kwargs):
    return meta_precision(**kwargs)

@_meta.register(Integer)
def _(c_type, **kwargs):
    return meta_integer(**kwargs)

@_meta.register(Time)
def _(c_type, **kwargs):
    return meta_time(**kwargs)

@_meta.register(Boolean)
def _(c_type, **kwargs):
    return meta_boolean(**kwargs)

# @TODO Replace autoloaded table declarations with expanded forms
#   - We'll have to look at database schema and make the tables
"""
class datafiles(Base):
    __table__ = Table('datafiles', metadata, autoload=True)

class instruments(Base):
    __table__ = Table('instruments_meta', metadata, autoload=True)

class targets(Base):
    __table__ = Table('targets_meta', metadata, autoload=True)

class keywords(Base):
    __table__ = Table('keywords', metadata, autoload=True)
"""

class meta_precision(Base):
    __tablename__ = 'meta_precision'
    upcid = Column(Integer, primary_key = True)
    typeid = Column(Integer)
    # @TODO column description?
    value = Column(Double)

class meta_time(Base):
    __tablename__ = 'meta_time'
    upcid = Column(Integer, primary_key = True)
    typeid = Column(Integer)
    # @TODO column description?
    value = Column(Time)

class meta_string(Base):
    __tablename__ = 'meta_string'
    upcid = Column(Integer, primary_key = True)
    typeid = Column(Integer)
    # @TODO column description?
    value = Column(String)

class meta_integer(Base):
    __tablename__ = 'meta_integer'
    upcid = Column(Integer, primary_key = True)
    typeid = Column(Integer)
    # @TODO column description?
    value = Column(Integer)

class meta_boolean(Base):
    __tablename__ = 'meta_boolean'
    upcid = Column(Integer, primary_key = True)
    typeid = Column(Integer)
    # @TODO column description?
    value = Column(Boolean)

class meta_geometry(Base):
    __tablename__ = 'meta_geometry'
    upcid = Column(Integer, primary_key=True)
    typeid = Column(Integer)
    value = Column(Geometry('geometry'))

if __name__ == "__main__":
    t1 = meta('Boolean', upcid=1, typeid=0, value=True)
    t2 = meta('string', upcid=2, typeid=1, value = 'StringExample')
    t3 = meta('Integer', upcid=3, typeid=2, value = 1234)
    t4 = meta('double', upcid=4, typeid=3, value = 5.6789)
