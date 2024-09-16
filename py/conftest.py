from pytest import main, FixtureRequest, fixture
from pyodbc import Connection, connect as odbc_connect
from mrtest import DbModel, DbMock, DbCmd
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


if __name__ == '__main__':
    cn = _create_examples_connection()
    cn.close()
    print('done')