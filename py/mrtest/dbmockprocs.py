from mrtest import DbCmd
from sqlalchemy import Connection
from typing import Any
from datetime import datetime, date, time
from uuid import UUID

class MockProcedures:
    def __init__(self, cnOrStr: (Connection | str)):
        self.cnOrStr = cnOrStr

    def usp_mock_table(self, schema_name: str, table_name: str) -> DbCmd:
        sql = """
DECLARE @_return_value INT;
DECLARE @schema_name nvarchar(512) = ?;
DECLARE @table_name nvarchar(512) = ?;

EXECUTE @_return_value = [mrtest].[usp_mock_table]
    @schema_name = @schema_name
    ,@table_name = @table_name;
"""
        return DbCmd(self.cnOrStr, sql, [ schema_name, table_name ])

    def usp_restore_table(self, schema_name: str, table_name: str) -> DbCmd:
        sql = """
DECLARE @_return_value INT;
DECLARE @schema_name nvarchar(512) = ?;
DECLARE @table_name nvarchar(512) = ?;

EXECUTE @_return_value = [mrtest].[usp_restore_table]
    @schema_name = @schema_name
    ,@table_name = @table_name;
"""
        return DbCmd(self.cnOrStr, sql, [ schema_name, table_name ])

    def usp_restore_all_tables(self) -> DbCmd:
        sql = """
DECLARE @_return_value INT;

EXECUTE @_return_value = [mrtest].[usp_restore_all_tables];
"""
        return DbCmd(self.cnOrStr, sql, [  ])

