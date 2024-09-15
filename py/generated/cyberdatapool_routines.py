from mrtest import DbCmd
from pyodbc import Connection
from typing import Any
from datetime import datetime, date, time
from uuid import UUID

class CyberDataPoolDboRoutines:
    def __init__(self, cnOrStr: (Connection | str)):
        self.cnOrStr = cnOrStr

    def Ten_Most_Expensive_Products(self) -> DbCmd:
        sql = """
DECLARE @_return_value INT;

EXECUTE @_return_value = [dbo].[Ten Most Expensive Products];
"""
        return DbCmd(self.cnOrStr, sql, [  ])

    def Employee_Sales_by_Country(self, Beginning_Date: datetime, Ending_Date: datetime) -> DbCmd:
        sql = """
DECLARE @_return_value INT;
DECLARE @Beginning_Date datetime = ?;
DECLARE @Ending_Date datetime = ?;

EXECUTE @_return_value = [dbo].[Employee Sales by Country]
    @Beginning_Date = @Beginning_Date
    ,@Ending_Date = @Ending_Date;
"""
        return DbCmd(self.cnOrStr, sql, [ Beginning_Date, Ending_Date ])

    def Sales_by_Year(self, Beginning_Date: datetime, Ending_Date: datetime) -> DbCmd:
        sql = """
DECLARE @_return_value INT;
DECLARE @Beginning_Date datetime = ?;
DECLARE @Ending_Date datetime = ?;

EXECUTE @_return_value = [dbo].[Sales by Year]
    @Beginning_Date = @Beginning_Date
    ,@Ending_Date = @Ending_Date;
"""
        return DbCmd(self.cnOrStr, sql, [ Beginning_Date, Ending_Date ])

    def CustOrdersDetail(self, OrderID: int) -> DbCmd:
        sql = """
DECLARE @_return_value INT;
DECLARE @OrderID int = ?;

EXECUTE @_return_value = [dbo].[CustOrdersDetail]
    @OrderID = @OrderID;
"""
        return DbCmd(self.cnOrStr, sql, [ OrderID ])

    def CustOrdersOrders(self, CustomerID: str) -> DbCmd:
        sql = """
DECLARE @_return_value INT;
DECLARE @CustomerID nchar(10) = ?;

EXECUTE @_return_value = [dbo].[CustOrdersOrders]
    @CustomerID = @CustomerID;
"""
        return DbCmd(self.cnOrStr, sql, [ CustomerID ])

    def CustOrderHist(self, CustomerID: str) -> DbCmd:
        sql = """
DECLARE @_return_value INT;
DECLARE @CustomerID nchar(10) = ?;

EXECUTE @_return_value = [dbo].[CustOrderHist]
    @CustomerID = @CustomerID;
"""
        return DbCmd(self.cnOrStr, sql, [ CustomerID ])

    def SalesByCategory(self, CategoryName: str, OrdYear: str) -> DbCmd:
        sql = """
DECLARE @_return_value INT;
DECLARE @CategoryName nvarchar(30) = ?;
DECLARE @OrdYear nvarchar(8) = ?;

EXECUTE @_return_value = [dbo].[SalesByCategory]
    @CategoryName = @CategoryName
    ,@OrdYear = @OrdYear;
"""
        return DbCmd(self.cnOrStr, sql, [ CategoryName, OrdYear ])

    def usp_test(self) -> DbCmd:
        sql = """
DECLARE @_return_value INT;

EXECUTE @_return_value = [dbo].[usp_test];
"""
        return DbCmd(self.cnOrStr, sql, [  ])

