from sqlalchemy import create_engine
from pytest import FixtureRequest, fixture
from mrtest import DbMock, fixture_connection, fixture_mock, Session

_cn_url = "mssql+pyodbc://sqlaircyber2.munichre.com/dev_cyber_datapool?driver=ODBC+Driver+17+for+SQL+Server"
_cn = None

@fixture
def connection(request: FixtureRequest):
    global _cn, _cn_url
    _cn = fixture_connection(request, _cn, _cn_url)
    return _cn

@fixture
def mock(request: FixtureRequest, connection):
    return fixture_mock(request, connection)

@fixture
def session(request: FixtureRequest, connection, mock):
    Session._try_set(connection, mock)
    return Session()
