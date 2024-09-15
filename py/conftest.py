
from pytest import main, FixtureRequest, fixture
from pyodbc import Connection, connect as odbc_connect
from mrtest import DbModel, DbMock
import mrtest.myconfig as myconfig
from generated.examples_routines import TstDbExamplesRoutines
# from generated.cyberdatapool_routines import CyberDataPoolDboRoutines



####################################################################
# northwind examples

_examples_cnstr = 'DRIVER={SQL Server};SERVER=.;DATABASE=testdb;Trusted_Connection=Yes;'
_examples_cn = None

@fixture
def examples_connection(request: FixtureRequest):
    global _examples_cn, _examples_cnstr

    close = False
    if _examples_cn == None:
        _examples_cn = odbc_connect(_examples_cnstr)
        close = True

    def tear_down():
        global _examples_cn
        if close:
            _examples_cn.close()
            _examples_cn = None
    request.addfinalizer(tear_down)

    return _examples_cn    

@fixture
def examples_routines(examples_connection):
    return TstDbExamplesRoutines(examples_connection)

@fixture
def examples_mock(request: FixtureRequest, examples_connection):
    result = DbMock(examples_connection)
    
    def tear_down():
        result.restore_all()
    request.addfinalizer(tear_down)

    return result

####################################################################
# sandbox

_sbx_cn: Connection = None

@fixture
def sandbox_connection(request: FixtureRequest):
    global _sbx_cn

    close = False
    if _sbx_cn == None:
        _sbx_cn = odbc_connect(myconfig.sandbox_cnstr)
        close = True

    def tear_down():
        if close: _sbx_cn.close()
        _sbx_cn = None
    request.addfinalizer(tear_down)

    return _sbx_cn    

@fixture
def sandbox_routines(request: FixtureRequest, sandbox_connection: Connection):
    from generated.sandbox_routines import SandboxDboRoutines
    result = SandboxDboRoutines(sandbox_connection)
    return result

@fixture
def sandbox_mock(request: FixtureRequest, sandbox_connection: Connection):
    result = DbMock(sandbox_connection)
    
    def tear_down():
        result.restore_all()
    request.addfinalizer(tear_down)

    return result

####################################################################
# cyber_datapool

_cyber_cnstr = myconfig.cyber_cnstr
_cyber_cn = None

@fixture
def cyber_connection(request: FixtureRequest):
    global _cyber_cn, _cyber_cnstr

    close = False
    if _cyber_cn == None:
        _cyber_cn = odbc_connect(_cyber_cnstr)
        close = True

    def tear_down():
        global _cyber_cn
        if close:
            _cyber_cn.close()
            _cyber_cn = None
    request.addfinalizer(tear_down)

    return _examples_cn    

@fixture
def model(request, cyber_connection):
    result = DbModel(cyber_connection)
    return result
