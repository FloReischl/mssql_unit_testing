
from pytest import main, FixtureRequest, fixture
from pyodbc import Connection, connect as odbc_connect
from mrtest import DbModel, DbExec
from mrtest import DbMock
import mrtest.myconfig as myconfig
# from generated.cyberdatapool_routines import CyberDataPoolDboRoutines

cnstr = myconfig.cyber_cnstr

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

@fixture
def dbx(request):
    global cnstr

    result = DbExec(cnstr)

    def tear_down():
        result.cn.close()
    
    return result

@fixture
def model(request, dbx):
    result = DbModel(dbx)
    return result
