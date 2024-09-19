from sqlalchemy import Connection, text, create_engine
from .utils import quote
from .dbmockprocs import MockProcedures
from warnings import warn

class _InnerMock:
    def __init__(self, con: Connection) -> None:
        self.cn = con if isinstance(con, Connection) else create_engine(str(con)).connect()
        self.procs = MockProcedures(con)
        self.mocked_tables = set()

    def mock_table(self, schema: str, name: str):
        key = self._key(schema, name)
        assert not key in self.mocked_tables, 'table is already mocked'

        with self.cn.execute(text("EXECUTE mrtest.usp_mock_table @schema_name = :schema_name, @table_name = :table_name"), { "schema_name": schema, "table_name": name }):
            self.mocked_tables.add(key)

    def restore_all_tables(self):
        removed = []
        exlist = list[Exception]()

        for key in self.mocked_tables:
            try:
                self.restore_table(schema=key[0], name=key[1])
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
        with self.cn.execute(text("EXECUTE mrtest.usp_restore_all_tables")) as cur_res:
            for row in cur_res.fetchall():
                warn(f"Fixed table {row.schema_name}.{row.table_name}")

    def restore_table(self, schema: str, name: str):
        with self.cn.execute(text("EXECUTE mrtest.usp_restore_table @schema_name = :schema_name, @table_name = :table_name"), { "schema_name": schema, "table_name": name }):
            pass
    
    def _key(self, schema: str, name: str) -> tuple[str, str]:
        return (schema, name)

class DbMock:
    """
    Mock database objects for testing.
    """
    def __init__(self, cn: (Connection | str)) -> None:
        self._inner = _InnerMock(cn)
        self.cn = self._inner.cn

    def mock_table(self, schema: str, name: str):
        """
        Mock a specific database table
        """
        self._inner.mock_table(schema, name)

    def restore_all_tables(self):
        """
        restore all tables that where previously mocked.
        """
        self._inner.restore_all_tables()
    
    def restore_table(self, schema: str, name: str):
        """
        restore a specific table.
        """
        self._inner.restore_table(schema=schema, name=name)

    def reset_database(self):
        """
        Rest the full database to restore possibly not correctly restored tables from previous test runs.
        
        This happens sometimes, when a table was mocked within a test but the test debugger was stopped without rstoring the table.
        """
        self._inner.reset_database()

class DbMockScope:
    """
    Mock database objects for testing within a :with scope
    """
    def __init__(self, cn: (Connection | str)) -> None:
        self.inner = _InnerMock(cn)

    def mock_table(self, schema: str, name: str):
        self.inner.mock_table(schema, name)

    def restore_all_tables(self):
        self.inner.restore_all_tables()
    
    def restore_table(self, schema: str, name: str):
        self.inner.restore_table(schema=schema, name=name)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.inner.restore_all_tables()

