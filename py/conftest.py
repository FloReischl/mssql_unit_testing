
from pytest import main, FixtureRequest, fixture
from pyodbc import connect as odbc_connect
from mrtest import DbModel, DbExec
import mrtest.myconfig as myconfig
from generated.cyberdatapool_routines import CyberDataPoolDboRoutines

cnstr = myconfig.cyber_cnstr

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

@fixture
def datapool_dbo_procs(request):
    global cnstr
    result = CyberDataPoolDboRoutines(cnstr=cnstr)

@fixture
def sandbox_routines(request: FixtureRequest):
    from generated.sandbox_routines import SandboxDboRoutines
    
    cnstr = myconfig.sandbox_cnstr
    cn = odbc_connect(cnstr)
    result = SandboxDboRoutines(cn)

    def tear_down():
        cn.close()
    
    request.addfinalizer(tear_down)

    return result

@fixture
def sandbox_connection(request: FixtureRequest):
    cnstr = myconfig.sandbox_cnstr
    cn = odbc_connect(cnstr)

    def tear_down():
        cn.close()
    
    request.addfinalizer(tear_down)
    return cn

@fixture
def sandbox_dbx(request: FixtureRequest):
    cnstr = myconfig.sandbox_cnstr
    
    cn = odbc_connect(cnstr)
    result = DbExec(cn)

    def tear_down():
        cn.close()

    request.addfinalizer(tear_down)
    return result