from mrtest import DbMock, DbCmd
import pytest
import pandas as pd
from generated.examples_procedures import TstDbExamplesProcedures

class TestExamplesCustomers:
    def test_usp_get_orders_for_customer(self, examples_procs: TstDbExamplesProcedures, examples_mock: DbMock):
        examples_mock.mock_table("examples", "Shippers")
        df = examples_procs.usp_insert_shipper("test-shipper", phone=None).exec_df()
        assert df.return_value.iloc[0] == 1

    def test_insert(self, examples_connection, examples_mock: DbMock):
        examples_mock.mock_table("examples", "Shippers")

        actual = DbCmd(examples_connection, f"select ShipperID, CompanyName, Phone from examples.Shippers").exec_df()
        expected = pd.DataFrame(columns=[ "ShipperID", "CompanyName", "Phone" ], data=[["1", "test-company", "123456"]])
        pd.testing.assert_frame_equal(expected, actual, check_dtype=False)
        pass

    # def test_usp_insert_category(self, examples_procs: TstDbExamplesProcedures, examples_mock: DbMock):
    #     examples_mock.mock_table(Categories.Categories_SchemaName, Categories.Categories_TableName)
    #     df = examples_procs.usp_insert_category("test-cat", "test-desc", None).exec_df()
    #     assert len(df) == 1
    #     assert (df['@_return_value'] == 1).all()

if __name__ == '__main__':
    pytest.main([__file__
                # , '-k', 'test_load_data'
                 ])
