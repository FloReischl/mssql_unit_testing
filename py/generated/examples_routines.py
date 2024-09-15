from mrtest import DbCmd
from pyodbc import Connection
from typing import Any
from datetime import datetime, date, time
from uuid import UUID

class TstDbExamplesRoutines:
    def __init__(self, cnOrStr: (Connection | str)):
        self.cnOrStr = cnOrStr

    def custom_sql(self, sql: str, params: Any = None):
        return DbCmd(self.cnOrStr, sql, params)

    def usp_insert_region(self, id: int, description: str) -> DbCmd:
        sql = """
DECLARE @_return_value INT;
DECLARE @id int = ?;
DECLARE @description nchar(100) = ?;

EXECUTE @_return_value = [examples].[usp_insert_region]
    @id = @id
    ,@description = @description;

SELECT @_return_value [@_return_value];
"""
        return DbCmd(self.cnOrStr, sql, [ id, description ])

    def usp_insert_category(self, name: str, description: str, picture: bytes) -> DbCmd:
        sql = """
DECLARE @_return_value INT;
DECLARE @name nvarchar(30) = ?;
DECLARE @description nvarchar(max) = ?;
DECLARE @picture varbinary = ?;

EXECUTE @_return_value = [examples].[usp_insert_category]
    @name = @name
    ,@description = @description
    ,@picture = @picture;

SELECT @_return_value [@_return_value];
"""
        return DbCmd(self.cnOrStr, sql, [ name, description, picture ])

    def usp_get_orders_for_customer(self, customerId: str) -> DbCmd:
        sql = """
DECLARE @_return_value INT;
DECLARE @customerId nchar(10) = ?;

EXECUTE @_return_value = [examples].[usp_get_orders_for_customer]
    @customerId = @customerId;

SELECT @_return_value [@_return_value];
"""
        return DbCmd(self.cnOrStr, sql, [ customerId ])

    def usp_get_customers_by_ids(self, ids: object) -> DbCmd:
        sql = """
DECLARE @_return_value INT;
DECLARE @ids tt_id_varchar50 = ?;

EXECUTE @_return_value = [examples].[usp_get_customers_by_ids]
    @ids = @ids;

SELECT @_return_value [@_return_value];
"""
        return DbCmd(self.cnOrStr, sql, [ ids ])

    def usp_precision_scale_params(self, datetime2: datetime, time: time, decimal: float) -> DbCmd:
        sql = """
DECLARE @_return_value INT;
DECLARE @datetime2 datetime2(5) = ?;
DECLARE @time time(3) = ?;
DECLARE @decimal decimal(5, 3) = ?;

EXECUTE @_return_value = [examples].[usp_precision_scale_params]
    @datetime2 = @datetime2
    ,@time = @time
    ,@decimal = @decimal;

SELECT @_return_value [@_return_value];
"""
        return DbCmd(self.cnOrStr, sql, [ datetime2, time, decimal ])

