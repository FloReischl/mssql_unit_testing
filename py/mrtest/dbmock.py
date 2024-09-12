import pyodbc
from pyodbc import connect as odbc_connect, Row
from .utils import quote
# from .sql import

class DbMock:
    def __init__(self, cnstr: str, unmock_existing: bool = False) -> None:
        self.cn = odbc_connect(cnstr)
        self.unmock_existing = unmock_existing
        self.faked_tables = dict()

    def mock_table(self, schema: str, table: str):
        # source_table = self.select_one(SELECT_OBJECT_BY_NAME_AND_SCHEMA, [ schema, table ])
        # assert source_table, f"original table {table} not found!"

        # mock_name = source_table.name + MOCK_POSTFIX

        # target_table = self.select_one(SELECT_OBJECT_BY_NAME_AND_SCHEMA, [schema, mock_name ])
        # assert not target_table, f"mock table {mock_name} already exists!"

        # full_name = f"{quote(schema)}.{quote(table)}"

        # raise Exception("self.execute_only(\"SELECT TOP(0) * INTO \")")

        # self.execute_only("EXECUTE sp_rename ?, ?", [ full_name, mock_name ])
        pass

    def select_one(self, sql: str, params) -> Row:
        with self.cn.execute(sql, params) as cur:
            return cur.fetchone()
    
    def execute_only(self, sql: str, params):
        with self.cn.execute(sql, params):
            pass
    
    def unmock_all(self):
        with self.cn.execute(sql.SELECT_ALL_MOCKED_OBJECTS) as cur:
            rows = cur.fetchall()
            for mocked in rows:
                print (mocked)
    
    def __enter__(self):
        if self.unmock_existing:
            self.unmock_all()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.unmock_all()

if __name__ == '__main__':
    import myconfig
    cnstr = myconfig.sandbox_cnstr
    with DbMock(cnstr=cnstr, unmock_existing=True) as mock:
        mock.mock_table('dbo', 'Dim_City')
