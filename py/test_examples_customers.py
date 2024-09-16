from mrtest import DbMock, DbCmd
import pytest, pyodbc
from pyodbc import Connection, BinaryNull
import pandas as pd
from mrtest import DataLoader
from generated.examples_procedures import TstDbExamplesProcedures
from generated.examples_tables import TstDbExamplesTables
import linq

Categories = TstDbExamplesTables.Categories

class TestExamplesCustomers:
    def test_select_customer(self, examples_connection: Connection):
        customerId = "ALFKI"
        df = DbCmd(examples_connection, "SELECT * FROM examples.Customers WHERE CustomerID = ?", [ customerId ]).exec_df()
        assert 1 == len(df)
        assert all(df.CustomerID == 'ALFKI')

    def test_usp_get_orders_for_customer(self, examples_procs: TstDbExamplesProcedures):
        df = examples_procs.usp_get_orders_for_customer("ALFKI").exec_df()
        assert not df.empty
        assert all(df.CustomerID == 'ALFKI')

    def test_usp_insert_category(self, examples_procs: TstDbExamplesProcedures, examples_mock: DbMock):
        examples_mock.mock_table(Categories.SchemaName, Categories.TableName)
        picture = pyodbc.BinaryNull # bytes('pseudo image', 'utf-8')
        df = examples_procs.usp_insert_category("test-cat", "test-desc", picture).exec_df()
        assert len(df) == 1
        assert (df['@_return_value'] == 1).all()

    def test_load_data(self, examples_connection: Connection):
        table = TstDbExamplesTables.Categories(examples_connection)
        df = pd.DataFrame([['cat1', 'desc1', BinaryNull],['cat2', 'desc2', BinaryNull]],columns=['CategoryName','Description', 'Picture'])
        loader = DataLoader()
        loader.load_df(table.insert, df)

if __name__ == '__main__':
    pytest.main([__file__
                # , '-k', 'test_load_data'
                 ])
