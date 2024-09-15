import pytest
from pyodbc import Connection
from mrtest import DbMock, DbCmd
from generated.sandbox_routines import SandboxDboRoutines

class Test_Dim_City:
    # raw odbc
    # def test_get_city_odbc(self, sandbox_connection: Connection):
    #     with sandbox_connection.execute('execute usp_get_city @city_id = ?', [ 24379 ]) as cur:
    #         rows = cur.fetchall()
    #         assert all(x.City == 'München' for x in rows)
    #         assert len(rows) == 1

    # # raw dataframe
    # def test_get_city_df(self, sandbox_connection):
    #     df = DbCmd(sandbox_connection, 'execute usp_get_city @city_id = ?', [ 24379 ]).exec_df()
    #     assert all(df.City == 'München')
    #     assert len(df) == 1

    # object oriented proxy
    def test_get_city(self, sandbox_routines: SandboxDboRoutines):
        df = sandbox_routines.usp_get_city(city_id=24379).df
        assert all(df.City == 'München')
        assert len(df) == 1

    def test_mock_insert(self, sandbox_routines: SandboxDboRoutines , sandbox_mock: DbMock):
        sandbox_mock.mock_table("dbo", "Dim_City")
        dbx = sandbox_mock.dbx
        df = dbx.get_df("INSERT INTO Dim_City (City, Create_Time, Change_Time, Changed_By) VALUES ('Rametnach', getdate(), getdate(), 'flo');")

        df = dbx.get_df("SELECT * FROM Dim_City")
        assert len(df) == 1
        assert all(df.City == 'Rametnach')



if __name__ == '__main__':
    pytest.main([__file__
                , '-k', 'test_mock_insert'
                 ])
