from mrtest import DbCmd
from sqlalchemy import Connection, text, create_engine
from typing import Any
from datetime import datetime, date, time
from uuid import UUID

class ExamplesProcedures:
    def __init__(self, cnOrUrl: (Connection | str)):
        self.cn = cnOrUrl if isinstance(cnOrUrl, Connection) else create_engine(str(cnOrUrl)).connect()

    def usp_insert_region(self, id: int, description: str) -> DbCmd:
        sql = """
DECLARE @_return_value INT;
DECLARE @id int = :id;
DECLARE @description nchar(100) = :description;

EXECUTE @_return_value = [examples].[usp_insert_region]
    @id = @id
    ,@description = @description;

SELECT @_return_value [return_value];
"""
        return DbCmd(self.cn, sql, { 'id': id, 'description': description })

    def usp_insert_category(self, name: str, description: str, picture: bytes) -> DbCmd:
        sql = """
DECLARE @_return_value INT;
DECLARE @name nvarchar(30) = :name;
DECLARE @description nvarchar(max) = :description;
DECLARE @picture varbinary = :picture;

EXECUTE @_return_value = [examples].[usp_insert_category]
    @name = @name
    ,@description = @description
    ,@picture = @picture;

SELECT @_return_value [return_value];
"""
        return DbCmd(self.cn, sql, { 'name': name, 'description': description, 'picture': picture })

    def usp_get_orders_for_customer(self, customerId: str) -> DbCmd:
        sql = """
DECLARE @_return_value INT;
DECLARE @customerId nchar(10) = :customerId;

EXECUTE @_return_value = [examples].[usp_get_orders_for_customer]
    @customerId = @customerId;

SELECT @_return_value [return_value];
"""
        return DbCmd(self.cn, sql, { 'customerId': customerId })

    def usp_get_customers_by_ids(self, ids: object) -> DbCmd:
        sql = """
DECLARE @_return_value INT;
DECLARE @ids tt_id_varchar50 = :ids;

EXECUTE @_return_value = [examples].[usp_get_customers_by_ids]
    @ids = @ids;

SELECT @_return_value [return_value];
"""
        return DbCmd(self.cn, sql, { 'ids': ids })

    def usp_precision_scale_params(self, datetime2: datetime, time: time, decimal: float) -> DbCmd:
        sql = """
DECLARE @_return_value INT;
DECLARE @datetime2 datetime2(5) = :datetime2;
DECLARE @time time(3) = :time;
DECLARE @decimal decimal(5, 3) = :decimal;

EXECUTE @_return_value = [examples].[usp_precision_scale_params]
    @datetime2 = @datetime2
    ,@time = @time
    ,@decimal = @decimal;

SELECT @_return_value [return_value];
"""
        return DbCmd(self.cn, sql, { 'datetime2': datetime2, 'time': time, 'decimal': decimal })

