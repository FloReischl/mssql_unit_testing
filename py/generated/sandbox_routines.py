from mrtest import DbExec, Result
import pyodbc
from typing import Any
from datetime import datetime, date, time
from uuid import UUID

class SandboxDboRoutines:
    def __init__(self, cn: (pyodbc.Connection | str)):
        self.dbx = DbExec(cn)

    def usp_ensure_temp_to_target_table_columns(self, target_table: str, temp_table: str) -> Result:
        sql = """
DECLARE @_return_value INT;
DECLARE @target_table sysname(256) = ?;
DECLARE @temp_table sysname(256) = ?;

EXECUTE @_return_value = [dbo].[usp_ensure_temp_to_target_table_columns]
    @target_table = @target_table
    ,@temp_table = @temp_table;
"""
        return self.dbx.get_result(sql, [ target_table, temp_table ])

    def usp_get_city(self, city_id: int) -> Result:
        sql = """
DECLARE @_return_value INT;
DECLARE @city_id bigint = ?;

EXECUTE @_return_value = [dbo].[usp_get_city]
    @city_id = @city_id;
"""
        return self.dbx.get_result(sql, [ city_id ])

