import pytest
import pandas as pd
from mrtest import Session
from mrtest.db_methods import *
from generated.cyberdatapool_procedures import CyberDataPoolDboProcedures
from io import StringIO
import uuid
from helpers import single

test_uuid = uuid.uuid4()

class Test_Dim_Client_Name:
    ############################################################
    # create mock tables and static test data
    def _arrange_data(self, session: Session, stg_csv: str):
        cn = session.cn

        # dbo.Dim_Client_Name
        mock_table("dbo", "Dim_Client_Name")
        csv = StringIO("""
Client_Name,Hierarchy_Level_ID,Parent_ID,Create_Time,Change_Time,Changed_By
Dummy1,1,611,2023-01-27 16:00:00.000,2023-01-30 11:31:50.000,test
""")
        df = pd.read_csv(csv, header=0)
        df.to_sql(schema="dbo", name="Dim_Client_Name", con=cn, if_exists='append', index=False)

        # stg.Dim_Client_Name
        mock_table("stg", "Dim_Client_Name")
        csv = StringIO(stg_csv)
        df = pd.read_csv(csv, names=['Client_ID','Crud_Operation','Client_Name', 'Hierarchy_Level_ID', 'Parent_ID'])
        df["Session_ID"] = test_uuid
        df.to_sql(schema="stg", name="Dim_Client_Name", con=cn, if_exists='append', index=False)

        # commit changes to db
        cn.commit()


    ############################################################
    def test_insert_succeeds(self, session: Session):
        # arrange
        stg_csv = "-1,I,MunichRe,0,0"
        self._arrange_data(session, stg_csv)

        # act
        procs = CyberDataPoolDboProcedures(session)
        procs.usp_merge_Dim_Client_Name(test_uuid, 'Client_Name').exec_no_read()

        # assert
        row = db_exec_single("SELECT * FROM dbo.Dim_Client_Name WHERE Client_Name = 'MunichRe'")
        assert row.Client_Name == "MunichRe"
        

    ############################################################
    def test_update_succeeds(self, session: Session):
        # arrange
        stg_csv = "1,U,Maintal Inc.,0,2"
        self._arrange_data(session, stg_csv)

        # act
        procs = CyberDataPoolDboProcedures(session)
        procs.usp_merge_Dim_Client_Name(test_uuid, 'Client_Name').exec_no_read()

        # assert
        row = db_exec_single("SELECT * FROM dbo.Dim_Client_Name WHERE Client_Name IN ('MunichRe', 'Maintal Inc.')")
        assert row.Client_Name == "Maintal Inc."
        

    ############################################################
    def test_update_different_uuid_no_change(self, session: Session):
        # arrange
        stg_csv = "1,U,Maintal Inc.,0,2"
        self._arrange_data(session, stg_csv)

        # act
        procs = CyberDataPoolDboProcedures(session)
        #                         \/  DIFFERENT UUID
        procs.usp_merge_Dim_Client_Name(uuid.uuid4(), 'Client_Name').exec_no_read()

        # assert
        row = db_exec_first_or_none("SELECT * FROM dbo.Dim_Client_Name WHERE Client_Name IN ('Frankfurt', 'Maintal Inc.')")
        assert not row


    ############################################################
    def test_delete_succeeds(self, session: Session):
        # arrange
        stg_csv = "2,D,Dummy2,0,2"
        self._arrange_data(session, stg_csv)

        # act
        procs = CyberDataPoolDboProcedures(session)
        procs.usp_merge_Dim_Client_Name(test_uuid, 'Client_Name').exec_no_read()

        # assert
        row = db_exec_first_or_none("SELECT * FROM dbo.Dim_Client_Name WHERE Client_Name = 'Dummy2'")
        assert not row


    ############################################################
    def _test_merge_succeeds(self, session: Session):
        # arrange
        stg_csv = """
1,U,Darmstadt,0,0
2,D,Berlin,0,0
-1,I,München,0,0
"""
        self._arrange_data(session, stg_csv)

        # act
        procs = CyberDataPoolDboProcedures(session)
        procs.usp_merge_Dim_Client_Name(test_uuid, 'Client_Name').exec_no_read()

        # assert
        df = db_exec_df("SELECT * FROM dbo.Dim_Client_Name WHERE Client_ID IN (1, 2, 5)")
        assert len(df) == 2
        assert single(df.Client_Name == "München")
        assert single(df.Client_Name == "Darmstadt")


    ############################################################
    def _test_merge_wrong_ids(self, session: Session):
        # arrange
        stg_csv = """
10,U,Darmstadt,0,0
20,D,Berlin,0,0
"""
        self._arrange_data(session, stg_csv)

        # act
        procs = CyberDataPoolDboProcedures(session)
        procs.usp_merge_Dim_Client_Name(test_uuid, 'Client_Name').exec_no_read()

        # assert
        # connection.commit()
        df = db_exec_df("SELECT * FROM dbo.Dim_Client_Name WHERE Client_Name IN ('Berlin', 'Darmstadt')")
        assert len(df) == 1
        assert any(df.Client_Name == "Berlin")

if __name__ == '__main__':
    pytest.main([__file__
                #, '-k', 'test_update_succeeds'
                 ])
