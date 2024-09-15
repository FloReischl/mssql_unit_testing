from mrtest import DbMock, DbCmd
import pytest, pyodbc
from pyodbc import Connection, BinaryNull
import pandas as pd
from mrtest import DataLoader
from generated.examples_routines import TstDbExamplesRoutines
from generated.examples_tables import TstDbExamplesTables

Categories = TstDbExamplesTables.Categories

class TestExamplesCustomers:
    def test_usp_get_orders_for_customer(self, examples_routines: TstDbExamplesRoutines):
        df = examples_routines.usp_get_orders_for_customer("ALFKI").exec_df()
        assert not df.empty
        assert all(df.CustomerID == 'ALFKI')

    def test_usp_insert_category(self, examples_routines: TstDbExamplesRoutines, examples_mock: DbMock):
        examples_mock.mock_table(Categories.Categories_SchemaName, Categories.Categories_TableName)
        picture = bytes('pseudo image', 'utf-8')
        df = examples_routines.usp_insert_category("test-cat", "test-desc", picture).exec_df()
        assert len(df) == 1
        assert (df['@_return_value'] == 1).all()

    def test_pass_params(self, examples_connection: Connection):
        sql = """
DECLARE @CategoryName nvarchar(30) = ?, @Description nvarchar(max) = ?, @Picture varbinary(max) = ?;
SELECT @CategoryName, @Description, @Picture
"""
        # b'\x124Vx'
        with examples_connection.execute(sql, [ 'foo', 'bar', pyodbc.BinaryNull ]) as cur:
            row = cur.fetchone()
            pass

    def test_load_data(self, examples_connection: Connection):
        table = TstDbExamplesTables.Categories(examples_connection)
        pd.read_csv()
        df = pd.DataFrame([['cat1', 'desc1', BinaryNull],['cat2', 'desc2', BinaryNull]],columns=['CategoryName','Description', 'Picture'])
        loader = DataLoader()
        loader.load_df(table.insert, df)



if __name__ == '__main__':
    pytest.main([__file__
                , '-k', 'test_load_data'
                 ])
