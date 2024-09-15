import pyodbc
from pyodbc import Connection, Row, connect as odbc_connect
from .utils import quote
from .dbmockprocs import MockProcedures

# TODO

class _InnerMock:
    def __init__(self, cnOrStr: (Connection | str)) -> None:
        self.cn = cnOrStr if isinstance(cnOrStr, Connection) else odbc_connect(cnOrStr)
        self.mock_procs = MockProcedures(cnOrStr)
        self.mocked_tables = set()

    def mock_table(self, schema: str, name: str):
        key = self._key(schema, name)
        assert not key in self.mocked_tables, 'table is already mocked'
        self.mock_procs.usp_mock_table(schema_name=schema, table_name=name).exec_no_read()
        self.mocked_tables.add(key)

    def restore_all(self):
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
    
    def unmock_table(self, schema: str, name: str):
        self.mock_procs.usp_restore_table(schema_name=schema, table_name=name).exec_no_read()
    
    def _key(self, schema: str, name: str) -> tuple[str, str]:
        return (schema, name)

class DbMock:
    
    def __init__(self, cn: (Connection | str)) -> None:
        self.inner = _InnerMock(cn)

    def mock_table(self, schema: str, name: str):
        self.inner.mock_table(schema, name)

    def restore_all(self):
        self.inner.restore_all()
    
    def unmock_table(self, schema: str, name: str):
        self.inner.unmock_table(schema=schema, name=name)

class DbMockScope:
    def __init__(self, cn: (Connection | str)) -> None:
        self.inner = _InnerMock(cn)

    def mock_table(self, schema: str, name: str):
        self.inner.mock_table(schema, name)

    def restore_all(self):
        self.inner.restore_all()
    
    def unmock_table(self, schema: str, name: str):
        self.inner.unmock_table(schema=schema, name=name)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.inner.restore_all()

if __name__ == '__main__':
    import myconfig
    cnstr = myconfig.sandbox_cnstr
    with DbMock(cnstr=cnstr, unmock_existing=True) as mock:
        mock.mock_table('dbo', 'Dim_City')
