import pytest
from pyodbc import Connection
from mrtest import DbExec
from generated.sandbox_routines import SandboxDboRoutines

class TestDbScripter:
    def test_get_city_odbc(self, sandbox_connection: Connection):
        with sandbox_connection.execute('execute usp_get_city @city_id = ?', [ 24379 ]) as cur:
            rows = cur.fetchall()
            assert all(x.City == 'München' for x in rows)
            assert len(rows) == 1

    def test_get_city_df(self, sandbox_dbx: DbExec):
        df = sandbox_dbx.get_df('execute usp_get_city @city_id = ?', [ 24379 ])
        assert all(df.City == 'München')
        assert len(df) == 1

    def test_get_city(self, sandbox_routines: SandboxDboRoutines):
        df = sandbox_routines.usp_get_city(city_id=24379).df
        assert all(df.City == 'München')
        assert len(df) == 1

if __name__ == '__main__':
    pytest.main([__file__
                #  , '-k'
                 ])
