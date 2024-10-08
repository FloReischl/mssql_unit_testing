SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

create   PROCEDURE [examples].[usp_get_orders_for_customer]  
    @customerId nchar(5)
as
    select 
            o.[OrderID]
            ,o.[CustomerID]
            ,o.[EmployeeID]
            ,o.[OrderDate]
            ,o.[RequiredDate]
            ,o.[ShippedDate]
            ,o.[ShipVia]
            ,o.[Freight]
            ,o.[ShipName]
            ,o.[ShipAddress]
            ,o.[ShipCity]
            ,o.[ShipRegion]
            ,o.[ShipPostalCode]
            ,o.[ShipCountry]
        from examples.Orders o 
        WHERE o.CustomerID = @customerId
        ;

    select
             od.OrderID
             ,od.ProductID
             ,od.UnitPrice
             ,od.Quantity
             ,od.Discount
        from examples.[Order Details] od 
        where exists (
            select *
            from Orders o
            where o.OrderID = od.OrderID
                and o.CustomerID = @customerId
        )
        ;
GO
