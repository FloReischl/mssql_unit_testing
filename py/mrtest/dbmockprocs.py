from mrtest import DbExec, Result
import pyodbc
from typing import Any
from datetime import datetime, date, time
from uuid import UUID

class MockProcedures:
    def __init__(self, cn: (pyodbc.Connection | str)):
        self.dbx = DbExec(cn)

    def usp_mock_table(self, schema_name: str, table_name: str) -> Result:
        sql = """
DECLARE @_return_value INT;
DECLARE @schema_name nvarchar(512) = ?;
DECLARE @table_name nvarchar(512) = ?;

EXECUTE @_return_value = [mrtest].[usp_mock_table]
    @schema_name = @schema_name
    ,@table_name = @table_name;
"""
        return self.dbx.get_result(sql, [ schema_name, table_name ])

    def usp_restore_table(self, schema_name: str, table_name: str) -> Result:
        sql = """
DECLARE @_return_value INT;
DECLARE @schema_name nvarchar(512) = ?;
DECLARE @table_name nvarchar(512) = ?;

EXECUTE @_return_value = [mrtest].[usp_restore_table]
    @schema_name = @schema_name
    ,@table_name = @table_name;
"""
        return self.dbx.get_result(sql, [ schema_name, table_name ])

    def usp_restore_all_tables(self) -> Result:
        sql = """
DECLARE @_return_value INT;

EXECUTE @_return_value = [mrtest].[usp_restore_all_tables];
"""
        return self.dbx.get_result(sql, [  ])

