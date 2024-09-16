import pytest
import pandas as pd
from pyodbc import Connection
from sqlalchemy import Connection as SAConnection
from sqlalchemy import create_engine, text
from mrtest import DbMock, DbCmd, DataLoader
# from generated.cyberdatapool_procedures import CyberDataPoolDboProcedures
# from generated.cyberdatapool_tables import CyberDataPoolDboTables
from generated.cyberdatapool_alchemy_procedures import CyberDataPoolDboAlchemyProcedures
from io import StringIO


class Test_Dim_City:
    def test_merge_dim_city(self, cdp_sa_con: SAConnection, cdp_sa_mock: DbMock):
        # arrange
        cdp_sa_mock.mock_table("dbo", "Dim_City")
        cdp_sa_mock.mock_table("stg", "Dim_City")

        csv = StringIO("""
City,Create_Time,Change_Time,Changed_By
Frankfurt,2024-09-01,2024-09-01,test
Berlin,2024-09-01,2024-09-01,test
Köln,2024-09-01,2024-09-01,test
""")
        df = pd.read_csv(csv, header=0)
        df.to_sql(schema="dbo", name="Dim_City", con=cdp_sa_con, if_exists='append', index=False)

        csv = StringIO("""
Session_ID,Crud_Operation,City_ID,City
0c57d525-0cfe-42ab-b789-4d1247bf8c7c,I,-1,München
0c57d525-0cfe-42ab-b789-4d1247bf8c7c,U,2,Hamburg
0c57d525-0cfe-42ab-b789-4d1247bf8c7c,D,3,Köln
""")
        df = pd.read_csv(csv, header=0)
        df.to_sql(schema="stg", name="Dim_City", con=cdp_sa_con, if_exists='append', index=False)

        procs = CyberDataPoolDboAlchemyProcedures(cdp_sa_con)
        procs.usp_merge_Dim_City('0c57d525-0cfe-42ab-b789-4d1247bf8c7c', 'City')

if __name__ == '__main__':
    pytest.main([__file__
                # , '-k', 'test_mock_insert'
                 ])
