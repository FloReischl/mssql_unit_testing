import pytest
import pandas as pd
from sqlalchemy import Connection, create_engine, text
from mrtest import DbMock, DbCmd
from generated.cyberdatapool_procedures import CyberDataPoolDboProcedures
from io import StringIO

class Test_Dim_City:
    def _arrange_data(self, cn: Connection, mock: DbMock, stg_csv: str):
        mock.mock_table("dbo", "Dim_City")
        mock.mock_table("stg", "Dim_City")

        csv = StringIO("""
City,Create_Time,Change_Time,Changed_By
Frankfurt,2024-09-01,2024-09-01,id-1
Berlin,2024-09-01,2024-09-01,id-2
Köln,2024-09-01,2024-09-01,id-3
Dresden,2024-09-01,2024-09-01,id-4
""")

        df = pd.read_csv(csv, header=0)
        df.to_sql(schema="dbo", name="Dim_City", con=cn, if_exists='append', index=False)

        csv = StringIO(stg_csv)
        df = pd.read_csv(csv, header=0)
        df.to_sql(schema="stg", name="Dim_City", con=cn, if_exists='append', index=False)
        cn.commit()

    def test_insert_succeeds(self, connection: Connection, mock: DbMock):
        # arrange
        stg_csv = """
City_ID,Session_ID,Crud_Operation,City
-1,0c57d525-0cfe-42ab-b789-4d1247bf8c7c,I,München
"""
        self._arrange_data(connection, mock, stg_csv)

        # act
        procs = CyberDataPoolDboProcedures(connection)
        procs.usp_merge_Dim_City('0c57d525-0cfe-42ab-b789-4d1247bf8c7c', 'City').exec_no_read()

        # assert
        row = DbCmd(connection, "SELECT * FROM dbo.Dim_City WHERE City = 'München'").exec_single()
        assert row.City == "München"
        assert row.City_ID == 5
    
    def test_update_succeeds(self, connection: Connection, mock: DbMock):
        # arrange
        stg_csv = """
City_ID,Session_ID,Crud_Operation,City
1,0c57d525-0cfe-42ab-b789-4d1247bf8c7c,U,München
"""
        self._arrange_data(connection, mock, stg_csv)

        # act
        procs = CyberDataPoolDboProcedures(connection)
        procs.usp_merge_Dim_City('0c57d525-0cfe-42ab-b789-4d1247bf8c7c', 'City').exec_no_read()

        # assert
        row = DbCmd(connection, "SELECT * FROM dbo.Dim_City WHERE City = 'München'").exec_single()
        assert row.City == "München"

        row = DbCmd(connection, "SELECT * FROM dbo.Dim_City WHERE City = 'Frankfurt'").exec_first_or_none()
        assert not row
    
    def test_delete_succeeds(self, connection: Connection, mock: DbMock):
        # arrange
        stg_csv = """
City_ID,Session_ID,Crud_Operation,City
2,0c57d525-0cfe-42ab-b789-4d1247bf8c7c,D,Berlin
"""
        self._arrange_data(connection, mock, stg_csv)

        # act
        procs = CyberDataPoolDboProcedures(connection)
        procs.usp_merge_Dim_City('0c57d525-0cfe-42ab-b789-4d1247bf8c7c', 'City').exec_no_read()

        # assert
        row = DbCmd(connection, "SELECT * FROM dbo.Dim_City WHERE City = 'Berlin'").exec_first_or_none()
        assert not row

    def test_merge_succeeds(self, connection: Connection, mock: DbMock):
        # arrange
        stg_csv = """
City_ID,Session_ID,Crud_Operation,City
1,0c57d525-0cfe-42ab-b789-4d1247bf8c7c,U,Darmstadt
2,0c57d525-0cfe-42ab-b789-4d1247bf8c7c,D,Berlin
-1,0c57d525-0cfe-42ab-b789-4d1247bf8c7c,I,München
"""
        self._arrange_data(connection, mock, stg_csv)

        # act
        procs = CyberDataPoolDboProcedures(connection)
        procs.usp_merge_Dim_City('0c57d525-0cfe-42ab-b789-4d1247bf8c7c', 'City').exec_no_read()

        # assert
        connection.commit()
        rows = DbCmd(connection, "SELECT * FROM dbo.Dim_City WHERE City_ID IN (1, 2, 5)").exec_df()
        assert len(rows) == 2
        assert not any(rows.City == "Berlin")
        assert not any(rows.City == "Frankfurt")
        assert any(rows.City == "München")
        assert any(rows.City == "Darmstadt")

    def test_merge_no_success(self, connection: Connection, mock: DbMock):
        # arrange
        stg_csv = """
City_ID,Session_ID,Crud_Operation,City
10,0c57d525-0cfe-42ab-b789-4d1247bf8c7c,U,Darmstadt
20,0c57d525-0cfe-42ab-b789-4d1247bf8c7c,D,Berlin
"""
        self._arrange_data(connection, mock, stg_csv)

        # act
        procs = CyberDataPoolDboProcedures(connection)
        procs.usp_merge_Dim_City('0c57d525-0cfe-42ab-b789-4d1247bf8c7c', 'City').exec_no_read()

        # assert
        connection.commit()
        rows = DbCmd(connection, "SELECT * FROM dbo.Dim_City WHERE City IN ('Berlin', 'Darmstadt')").exec_df()
        assert len(rows) == 1
        assert any(rows.City == "Berlin")


if __name__ == '__main__':
    pytest.main([__file__
                # , '-k', 'test_merge_no_success'
                 ])
