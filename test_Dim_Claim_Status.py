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

class Test_Dim_Claim_Status:
    # create mock tables and static, pre-defined data
    def _arrange_data(self, cn: Connection, mock: DbMock, stg_csv: str):
        mock.mock_table("dbo", "Dim_Claim_Status")
        mock.mock_table("stg", "Dim_Claim_Status")

        # dbo.Dim_Claim_Status
        csv = StringIO(
"""
Closed
Opened
Re-opened
""")
        df = pd.read_csv(csv, names=['Claim_Status'])
        df["Create_Time"] = datetime.datetime.today()
        df["Change_Time"] = datetime.datetime.today()
        df["Changed_By"] = "regression-test"
        df.to_sql(schema="dbo", name="Dim_Claim_Status", con=cn, if_exists='append', index=False)

        # stg.Dim_Claim_Status
        csv = StringIO(stg_csv)
        df = pd.read_csv(csv, names=['Claim_Status_ID', 'Crud_Operation', 'Claim_Status', 'Claim_Unified_Status_ID'])
        df["Session_ID"] = test_uuid
        df.to_sql(schema="stg", name="Dim_Claim_Status", con=cn, if_exists='append', index=False)

        # commit changes to db
        cn.commit()


    ############################################################
    def test_insert_succeeds(self, connection: Connection, mock: DbMock):
        # currently, writes into the table are not allowed
        pass
        # # arrange
        # self._arrange_data(connection, mock, "-1,I,Considered,")

        # # act
        # procs = CyberDataPoolDboProcedures(connection)
        # procs.usp_merge_Dim_Claim_Status(test_uuid, 'Claim_Status,Claim_Unified_Status_ID').exec_no_read()

        # # assert
        # row = DbCmd(connection, "SELECT * FROM dbo.Dim_Claim_Status WHERE Claim_Status = 'Considered'").exec_single()
        # assert row.Client_ID == 4
    

    ############################################################
    def test_update_succeeds(self, connection: Connection, mock: DbMock):
        # currently, writes into the table are not allowed
        pass
        # # arrange
        # self._arrange_data(connection, mock, "2,U,Replaced,666")

        # # act
        # procs = CyberDataPoolDboProcedures(connection)
        # procs.usp_merge_Dim_Claim_Status(test_uuid, 'Claim_Status,Claim_Unified_Status_ID').exec_no_read()

        # # assert
        # row = DbCmd(connection, "SELECT * FROM dbo.Dim_Claim_Status WHERE Client_ID = 2").exec_single()
        # assert row.Claim_Status == 'Replaced'
        

    ############################################################
    def test_delete_succeeds(self, connection: Connection, mock: DbMock):
        # currently, writes into the table are not allowed
        pass
        # # arrange
        # stg_csv = "3,D,x,"
        # self._arrange_data(connection, mock, stg_csv)

        # # act
        # procs = CyberDataPoolDboProcedures(connection)
        # procs.usp_merge_Dim_Claim_Status(test_uuid, 'Claim_Status,Claim_Unified_Status_ID').exec_no_read()

        # # assert
        # row = DbCmd(connection, "SELECT * FROM dbo.Dim_Claim_Status WHERE Client_ID = 3").exec_first_or_none()
        # assert not row


if __name__ == '__main__':
    pytest.main([__file__
                # , '-k', 'test_delete_succeeds'
                 ])
