from mrtest import DbMock, DbCmd
import pytest, pyodbc
from pyodbc import Connection, BinaryNull
import pandas as pd
from mrtest import DataLoader
from generated.examples_procedures import TstDbExamplesProcedures
from generated.examples_tables import TstDbExamplesTables
import linq

Shippers = TstDbExamplesTables.Shippers

class TestExamplesCustomers:
    def test_usp_get_orders_for_customer(self, examples_procs: TstDbExamplesProcedures, examples_mock: DbMock):
        examples_mock.mock_table(Shippers.SchemaName, Shippers.TableName)
        df = examples_procs.usp_insert_shipper("test-shipper", phone=None).exec_df()
        assert df.return_value.iloc[0] == 1

    def test_insert(self, examples_connection, examples_mock: DbMock):
        examples_mock.mock_table(Shippers.SchemaName, Shippers.TableName)
        table = Shippers(examples_connection)
        table.insert("test-company", "123456")

        actual = DbCmd(examples_connection, f"select * from {Shippers.QualifiedName}").exec_df()
        expected = pd.DataFrame(columns=[ Shippers.ShipperID, Shippers.CompanyName, Shippers.Phone ], data=[["1", "test-company", "123456"]])
        pd.testing.assert_frame_equal(expected, actual, check_dtype=False)
        pass

    # def test_usp_insert_category(self, examples_procs: TstDbExamplesProcedures, examples_mock: DbMock):
    #     examples_mock.mock_table(Categories.Categories_SchemaName, Categories.Categories_TableName)
    #     picture = pyodbc.BinaryNull # bytes('pseudo image', 'utf-8')
    #     df = examples_procs.usp_insert_category("test-cat", "test-desc", picture).exec_df()
    #     assert len(df) == 1
    #     assert (df['@_return_value'] == 1).all()

    # def test_load_data(self, examples_connection: Connection):
    #     table = TstDbExamplesTables.Categories(examples_connection)
    #     df = pd.DataFrame([['cat1', 'desc1', BinaryNull],['cat2', 'desc2', BinaryNull]],columns=['CategoryName','Description', 'Picture'])
    #     loader = DataLoader()
    #     loader.load_df(table.insert, df)

if __name__ == '__main__':
    pytest.main([__file__
                # , '-k', 'test_load_data'
                 ])
