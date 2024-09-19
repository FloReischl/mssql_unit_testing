import pytest
import pandas as pd
from sqlalchemy import Connection
from mrtest import DbMock, DbCmd
from generated.cyberdatapool_procedures import CyberDataPoolDboProcedures
from io import StringIO
from uuid import UUID, uuid4
from helpers import single

test_uuid = UUID("0c57d525-0cfe-42ab-b789-4d1247bf8c7c")

class Test_Dim_City:
    # create mock tables and static, pre-defined data
    def _arrange_data(self, cn: Connection, mock: DbMock, stg_csv: str):
        mock.mock_table("dbo", "Dim_City")
        mock.mock_table("stg", "Dim_City")

        # dbo.Dim_City
        csv = StringIO("""
City,Create_Time,Change_Time,Changed_By
Frankfurt,2024-09-01,2024-09-01,id-1
Berlin,2024-09-01,2024-09-01,id-2
Köln,2024-09-01,2024-09-01,id-3
Dresden,2024-09-01,2024-09-01,id-4
""")
        df = pd.read_csv(csv, header=0)
        df.to_sql(schema="dbo", name="Dim_City", con=cn, if_exists='append', index=False)

        # stg.Dim_City
        csv = StringIO(stg_csv)
        df = pd.read_csv(csv, names=['City_ID','Crud_Operation','City'])
        df["Session_ID"] = test_uuid
        df.to_sql(schema="stg", name="Dim_City", con=cn, if_exists='append', index=False)

        # commit changes to db
        cn.commit()


    ############################################################
    def test_insert_succeeds(self, connection: Connection, mock: DbMock):
        # arrange
        stg_csv = "-1,I,München"
        self._arrange_data(connection, mock, stg_csv)

        # act
        procs = CyberDataPoolDboProcedures(connection)
        procs.usp_merge_Dim_City(test_uuid, 'City').exec_no_read()

        # assert
        row = DbCmd(connection, "SELECT * FROM dbo.Dim_City WHERE City = 'München'").exec_single()
        assert row.City == "München"
        assert row.City_ID == 5
    

    ############################################################
    def test_update_succeeds(self, connection: Connection, mock: DbMock):
        # arrange
        stg_csv = "1,U,Maintal"
        self._arrange_data(connection, mock, stg_csv)

        # act
        procs = CyberDataPoolDboProcedures(connection)
        procs.usp_merge_Dim_City(test_uuid, 'City').exec_no_read()

        # assert
        row = DbCmd(connection, "SELECT * FROM dbo.Dim_City WHERE City IN ('Frankfurt', 'Maintal')").exec_single()
        assert row.City == "Maintal"
        

    ############################################################
    def test_update_different_uuid_no_change(self, connection: Connection, mock: DbMock):
        # arrange
        stg_csv = "1,U,Maintal"
        self._arrange_data(connection, mock, stg_csv)

        # act
        procs = CyberDataPoolDboProcedures(connection)
        #                         \/  DIFFERENT UUID
        procs.usp_merge_Dim_City(uuid4(), 'City').exec_no_read()

        # assert
        row = DbCmd(connection, "SELECT * FROM dbo.Dim_City WHERE City IN ('Frankfurt', 'Maintal')").exec_single()
        assert row.City == "Frankfurt"


    ############################################################
    def test_delete_succeeds(self, connection: Connection, mock: DbMock):
        # arrange
        stg_csv = "2,D,Berlin"
        self._arrange_data(connection, mock, stg_csv)

        # act
        procs = CyberDataPoolDboProcedures(connection)
        procs.usp_merge_Dim_City(test_uuid, 'City').exec_no_read()

        # assert
        row = DbCmd(connection, "SELECT * FROM dbo.Dim_City WHERE City = 'Berlin'").exec_first_or_none()
        assert not row


    ############################################################
    def test_merge_succeeds(self, connection: Connection, mock: DbMock):
        # arrange
        stg_csv = """
1,U,Darmstadt
2,D,Berlin
-1,I,München
"""
        self._arrange_data(connection, mock, stg_csv)

        # act
        procs = CyberDataPoolDboProcedures(connection)
        procs.usp_merge_Dim_City(test_uuid, 'City').exec_no_read()

        # assert
        connection.commit()
        rows = DbCmd(connection, "SELECT * FROM dbo.Dim_City WHERE City_ID IN (1, 2, 5)").exec_df()
        assert len(rows) == 2
        assert single(rows.City == "München")
        assert single(rows.City == "Darmstadt")


    ############################################################
    def test_merge_wrong_ids(self, connection: Connection, mock: DbMock):
        # arrange
        stg_csv = """
10,U,Darmstadt
20,D,Berlin
"""
        self._arrange_data(connection, mock, stg_csv)

        # act
        procs = CyberDataPoolDboProcedures(connection)
        procs.usp_merge_Dim_City(test_uuid, 'City').exec_no_read()

        # assert
        connection.commit()
        rows = DbCmd(connection, "SELECT * FROM dbo.Dim_City WHERE City IN ('Berlin', 'Darmstadt')").exec_df()
        assert len(rows) == 1
        assert any(rows.City == "Berlin")



if __name__ == '__main__':
    pytest.main([__file__
                #, '-k', 'test_merge_succeeds'
                 ])
