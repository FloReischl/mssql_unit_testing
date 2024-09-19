from typing import Iterable
from mrtest import DbMock, DbCmd
import pytest
from sqlalchemy import Connection
from generated.examples_procedures import ExamplesAlchemyProcedures
from io import StringIO
import pandas as pd

def single(iter: Iterable, /):
    result = False
    for i,x in enumerate(iter):
        if i == 1: return False
        result = True
    return result

class TestExamplesCustomers:
    def test_select_customer(self, examples_connection: Connection):
        customerId = "ALFKI"
        df = DbCmd(examples_connection, "SELECT * FROM examples.Customers WHERE CustomerID = :customer_id", { "customer_id": 'ALFKI'}).exec_df()
        assert single(df.CustomerID == 'ALFKI')

    def test_usp_get_orders_for_customer(self, examples_connection):
        procs = ExamplesAlchemyProcedures(examples_connection)
        df = procs.usp_get_orders_for_customer("ALFKI").exec_df()
        assert all(df.CustomerID == 'ALFKI')

    def test_usp_insert_category(self, examples_connection, examples_mock: DbMock):
        procs = ExamplesAlchemyProcedures(examples_connection)
        examples_mock.mock_table("examples", "Categories")
        df = procs.usp_insert_category("test-cat", "test-desc", None).exec_df()
        assert single(df['return_value'] == 1)
        # assert len(df) == 1
        # assert (df['return_value'] == 1).all()

    def test_load_data(self, examples_connection: Connection):
        csv = StringIO("""
CategoryName,Description
test-category1,descr1
test-category2,descr2
""")
        df = pd.read_csv(csv, header=0)
        df.to_sql(schema="examples", name="Categories", con=examples_connection, if_exists='append', index=False)

if __name__ == '__main__':
    pytest.main([__file__
                # , '-k', 'test_load_data'
                 ])
