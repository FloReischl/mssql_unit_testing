from pytest import main, FixtureRequest, fixture
from pyodbc import Connection, connect as odbc_connect
from mrtest import DbModel, DbMock, DbCmd, ConInfo
import myconfig
from generated.examples_procedures import TstDbExamplesProcedures
# from generated.cyberdatapool_routines import CyberDataPoolDboRoutines



####################################################################
# northwind examples

_examples_cnstr = 'DRIVER={SQL Server};SERVER=sqlaircyber2.munichre.com;DATABASE=dev_sandbox;Trusted_Connection=Yes;'
_examples_cn = None

def _create_examples_connection() -> Connection:
    return odbc_connect(_examples_cnstr)

@fixture
def examples_connection(request: FixtureRequest):
    global _examples_cn, _examples_cnstr

    close = False
    if _examples_cn == None:
        _examples_cn = _create_examples_connection() #odbc_connect(_examples_cnstr)
        close = True
        mock = DbMock(_examples_cn)
        mock.reset_database()

    def tear_down():
        global _examples_cn
        if close:
            _examples_cn.close()
            _examples_cn = None
    request.addfinalizer(tear_down)

    return _examples_cn    

@fixture
def examples_procs(examples_connection):
    return TstDbExamplesProcedures(examples_connection)

@fixture
def examples_mock(request: FixtureRequest, examples_connection):
    result = DbMock(examples_connection)
    
    def tear_down():
        result.restore_tables()
    request.addfinalizer(tear_down)

    return result

####################################################################
# cyber_datapool

_cyber_coninfo = ConInfo("sqlaircyber2.munichre.com", "dev_cyber_datapool")
# _cyber_cnstr = 'DRIVER={SQL Server};SERVER=sqlaircyber2.munichre.com;DATABASE=dev_cyber_datapool;Trusted_Connection=Yes;'

_cyber_sa_con = None

@fixture
def cdp_sa_con(request: FixtureRequest):
    from sqlalchemy import create_engine
    global _cyber_sa_con, _cyber_coninfo

    close = False
    if _cyber_sa_con == None:
        url = _cyber_coninfo.alchemy_url()
        engine = create_engine(url)
        _cyber_sa_con = engine.connect()
        close = True
        mock = DbMock(_cyber_sa_con)
        mock.reset_database()

    def tear_down():
        global _cyber_sa_con
        if close:
            _cyber_sa_con.close()
            _cyber_sa_con = None
    request.addfinalizer(tear_down)

    return _cyber_sa_con

@fixture
def cdp_sa_mock(request: FixtureRequest, cdp_sa_con):
    result = DbMock(cdp_sa_con)
    
    def tear_down():
        result.restore_tables()
    request.addfinalizer(tear_down)

    return result

_cyber_cn = None

@fixture
def cdp_connection(request: FixtureRequest):
    global _cyber_cn, _cyber_coninfo

    close = False
    if _cyber_cn == None:
        cnstr = _cyber_coninfo.odbc_cnstr()
        _cyber_cn = odbc_connect(cnstr)
        close = True
        mock = DbMock(_cyber_cn)
        mock.reset_database()

    def tear_down():
        global _cyber_cn
        if close:
            _cyber_cn.close()
            _cyber_cn = None
    request.addfinalizer(tear_down)

    return _cyber_cn    

@fixture
def cdp_mock(request: FixtureRequest, cdp_connection):
    result = DbMock(cdp_connection)
    
    def tear_down():
        result.restore_tables()
    request.addfinalizer(tear_down)

    return result

if __name__ == '__main__':
    cn = _create_examples_connection()
    cn.close()
    print('done')