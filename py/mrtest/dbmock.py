import pyodbc
from pyodbc import Connection, Row, connect as odbc_connect
from .utils import quote
from .dbmockprocs import MockProcedures
from warnings import warn

# TODO

class _InnerMock:
    def __init__(self, cnOrStr: (Connection | str)) -> None:
        self.cn = cnOrStr if isinstance(cnOrStr, Connection) else odbc_connect(cnOrStr)
        self.procs = MockProcedures(cnOrStr)
        self.mocked_tables = set()

    def mock_table(self, schema: str, name: str):
        key = self._key(schema, name)
        assert not key in self.mocked_tables, 'table is already mocked'
        self.procs.usp_mock_table(schema_name=schema, table_name=name).exec_no_read()
        self.mocked_tables.add(key)

    def restore_tables(self):
        removed = []
        exlist = list[Exception]()

        for key in self.mocked_tables:
            try:
                key = next(x for x in self.mocked_tables)
                self.unmock_table(schema=key[0], name=key[1])
                removed.append(key)
            except Exception as ex:
                exlist.append(ex)

        for key in removed:
            self.mocked_tables.remove(key)

        if len(exlist):
            raise Exception(('one or more errors occurred while trying to restore mock tables!', exlist))

    def reset_database(self):
        """
        Fix all pending mock objects in the database.
        """
        df = self.procs.usp_restore_all_tables().exec_df()

        for i,row in df.iterrows():
            warn(f"Fixed table {row['schema_name']}.{row['table_name']}")

    def unmock_table(self, schema: str, name: str):
        self.procs.usp_restore_table(schema_name=schema, table_name=name).exec_no_read()
    
    def _key(self, schema: str, name: str) -> tuple[str, str]:
        return (schema, name)

class DbMock:
    
    def __init__(self, cn: (Connection | str)) -> None:
        self.inner = _InnerMock(cn)

    def mock_table(self, schema: str, name: str):
        self.inner.mock_table(schema, name)

    def restore_tables(self):
        self.inner.restore_tables()
    
    def unmock_table(self, schema: str, name: str):
        self.inner.unmock_table(schema=schema, name=name)

    def reset_database(self):
        self.inner.reset_database()

class DbMockScope:
    def __init__(self, cn: (Connection | str)) -> None:
        self.inner = _InnerMock(cn)

    def mock_table(self, schema: str, name: str):
        self.inner.mock_table(schema, name)

    def restore_all(self):
        self.inner.restore_tables()
    
    def unmock_table(self, schema: str, name: str):
        self.inner.unmock_table(schema=schema, name=name)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.inner.restore_tables()

