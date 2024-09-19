import pytest
import pandas as pd
from sqlalchemy import Connection
from mrtest import DbMock, DbCmd
from generated.cyberdatapool_procedures import CyberDataPoolDboProcedures
from io import StringIO
from uuid import UUID, uuid4
from helpers import single
import datetime

test_uuid = UUID("0c57d525-0cfe-42ab-b789-4d1247bf8c7c")

class Test_Dim_Company_Name_Clean:
    # create mock tables and static, pre-defined data
    def _arrange_data(self, cn: Connection, mock: DbMock, stg_csv: str):
        mock.mock_table("dbo", "Dim_Company_Name_Clean")
        mock.mock_table("stg", "Dim_Company_Name_Clean")

        # dbo.Dim_Company_Name_Clean
        csv = StringIO("""
company1,Company 1
CoMpAnY2,
""")
        df = pd.read_csv(csv, names=['Company_Name_ClientInfo', 'Company_Name_Clean'])
        df["Create_Time"] = datetime.datetime.today()
        df["Change_Time"] = datetime.datetime.today()
        df["Changed_By"] = "regression-test"
        df.to_sql(schema="dbo", name="Dim_Company_Name_Clean", con=cn, if_exists='append', index=False)

        # stg.Dim_Company_Name_Clean
        csv = StringIO(stg_csv)
        df = pd.read_csv(csv, names=['Company_Name_Clean_ID','Crud_Operation','Company_Name_ClientInfo', 'Company_Name_Clean'])
        df["Session_ID"] = test_uuid
        df.to_sql(schema="stg", name="Dim_Company_Name_Clean", con=cn, if_exists='append', index=False)

        # commit changes to db
        cn.commit()


    ############################################################
    def test_insert_succeeds(self, connection: Connection, mock: DbMock):
        # arrange
        stg_csv = "-1,I,Dirty Name,Clean Name"
        self._arrange_data(connection, mock, stg_csv)

        # act
        procs = CyberDataPoolDboProcedures(connection)
        procs.usp_merge_Dim_Company_Name_Clean(test_uuid, 'Company_Name_Clean').exec_no_read()

        # assert
        row = DbCmd(connection, "SELECT * FROM dbo.Dim_Company_Name_Clean WHERE Company_Name_Clean = 'Clean Name'").exec_single()
        assert row.Company_Name_Clean_ID == 3
    

    ############################################################
    def test_update_succeeds(self, connection: Connection, mock: DbMock):
        # arrange
        stg_csv = "2,U,Ignored,Awesome Name"
        self._arrange_data(connection, mock, stg_csv)

        # act
        procs = CyberDataPoolDboProcedures(connection)
        procs.usp_merge_Dim_Company_Name_Clean(test_uuid, 'Company_Name_Clean').exec_no_read()

        # assert
        row = DbCmd(connection, "SELECT * FROM dbo.Dim_Company_Name_Clean WHERE Company_Name_Clean_ID = 2").exec_single()
        assert row.Company_Name_ClientInfo != 'Ignored'
        assert row.Company_Name_Clean == 'Awesome Name'
        

    ############################################################
    def test_delete_succeeds(self, connection: Connection, mock: DbMock):
        # arrange
        stg_csv = "1,D,foo,bar"
        self._arrange_data(connection, mock, stg_csv)

        # act
        procs = CyberDataPoolDboProcedures(connection)
        procs.usp_merge_Dim_Company_Name_Clean(test_uuid, 'Company_Name_Clean').exec_no_read()

        # assert
        row = DbCmd(connection, "SELECT * FROM dbo.Dim_Company_Name_Clean WHERE Company_Name_Clean_ID = 1").exec_first_or_none()
        assert not row


    ############################################################
    def test_merge_succeeds(self, connection: Connection, mock: DbMock):
        # arrange
        stg_csv = """
1,U,foo,Clean Name
2,D,x,y
-1,I,ugly,beautiful
"""
        self._arrange_data(connection, mock, stg_csv)

        # act
        procs = CyberDataPoolDboProcedures(connection)
        procs.usp_merge_Dim_Company_Name_Clean(test_uuid, 'Company_Name_Clean').exec_no_read()

        # assert
        connection.commit()
        df = DbCmd(connection, "SELECT * FROM dbo.Dim_Company_Name_Clean WHERE Company_Name_Clean_ID IN (1, 2, 3)").exec_df()
        assert len(df) == 2
        assert 1 == len(df.query("Company_Name_Clean == 'Clean Name' and Company_Name_Clean_ID == 1"))
        assert 1 == len(df.query("Company_Name_Clean == 'beautiful' and Company_Name_Clean_ID == 3"))


if __name__ == '__main__':
    pytest.main([__file__
                # , '-k', 'test_merge_succeeds'
                 ])
