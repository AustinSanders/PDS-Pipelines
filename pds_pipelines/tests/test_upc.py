from datetime import datetime
import json

import pytest
import sqlalchemy
from unittest.mock import MagicMock, patch

from pds_pipelines.db import db_connect

@pytest.fixture
def tables():
    _, engine = db_connect('test')
    return engine.table_names()

@pytest.fixture
def session(tables, request):
    Session, _ = db_connect('test')
    session = Session()

    def cleanup():
        session.rollback()  # Necessary because some tests intentionally fail
        for t in reversed(tables):
            # Skip the srid table
            if t != 'spatial_ref_sys':
                session.execute(f'TRUNCATE TABLE {t} CASCADE')
            # Reset the autoincrementing
            if t in ['Datafiles', 'Instruments', 'Targets']:
                session.execute(f'ALTER SEQUENCE {t}_id_seq RESTART WITH 1')
        session.commit()

    request.addfinalizer(cleanup)

    return session
