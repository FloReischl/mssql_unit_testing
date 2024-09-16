from mrtest import DbCmd
from pyodbc import Connection
from typing import Any
from datetime import datetime, date, time
from uuid import UUID

class TstDbExamplesProcedures:
    def __init__(self, cnOrStr: (Connection | str)):
        self.cnOrStr = cnOrStr

    def custom_sql(self, sql: str, params: Any = None):
        return DbCmd(self.cnOrStr, sql, params)

    def usp_get_customers_by_ids(self, ids: object) -> DbCmd:
        sql = """
DECLARE @_return_value INT;
DECLARE @ids tt_id_varchar50 = ?;

EXECUTE @_return_value = [examples].[usp_get_customers_by_ids]
    @ids = @ids;

SELECT @_return_value [return_value];
"""
        return DbCmd(self.cnOrStr, sql, [ ids ])

    def usp_get_orders_for_customer(self, customerId: str) -> DbCmd:
        sql = """
DECLARE @_return_value INT;
DECLARE @customerId nchar(10) = ?;

EXECUTE @_return_value = [examples].[usp_get_orders_for_customer]
    @customerId = @customerId;

SELECT @_return_value [return_value];
"""
        return DbCmd(self.cnOrStr, sql, [ customerId ])

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

SELECT @_return_value [return_value];
"""
        return DbCmd(self.cnOrStr, sql, [ name, description, picture ])

    def usp_insert_region(self, id: int, description: str) -> DbCmd:
        sql = """
DECLARE @_return_value INT;
DECLARE @id int = ?;
DECLARE @description nchar(100) = ?;

EXECUTE @_return_value = [examples].[usp_insert_region]
    @id = @id
    ,@description = @description;

SELECT @_return_value [return_value];
"""
        return DbCmd(self.cnOrStr, sql, [ id, description ])

    def usp_multi_result_and_out_param(self, customer_id: str, order_count: int) -> DbCmd:
        sql = """
DECLARE @_return_value INT;
DECLARE @customer_id varchar(50) = ?;
DECLARE @order_count int = ?;

EXECUTE @_return_value = [examples].[usp_multi_result_and_out_param]
    @customer_id = @customer_id
    ,@order_count = @order_count OUTPUT;

SELECT @_return_value [return_value], @order_count [order_count_out];
"""
        return DbCmd(self.cnOrStr, sql, [ customer_id, order_count ])

    def usp_insert_shipper(self, company_name: str, phone: str) -> DbCmd:
        sql = """
DECLARE @_return_value INT;
DECLARE @company_name nvarchar(80) = ?;
DECLARE @phone nvarchar(48) = ?;

EXECUTE @_return_value = [examples].[usp_insert_shipper]
    @company_name = @company_name
    ,@phone = @phone;

SELECT @_return_value [return_value];
"""
        return DbCmd(self.cnOrStr, sql, [ company_name, phone ])

