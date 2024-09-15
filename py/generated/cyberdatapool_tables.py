from mrtest import DbCmd
from pyodbc import Connection
from datetime import datetime, date, time
from uuid import UUID

class CyberDataPoolDboTables:
    class Employees:
        # table
        Employees_TableName = 'Employees'
        Employees_SchemaName = 'dbo'
        Employees_QualifiedName = '[dbo].[Employees]'
        # columns
        EmployeeID = 'EmployeeID'
        LastName = 'LastName'
        FirstName = 'FirstName'
        Title = 'Title'
        TitleOfCourtesy = 'TitleOfCourtesy'
        BirthDate = 'BirthDate'
        HireDate = 'HireDate'
        Address = 'Address'
        City = 'City'
        Region = 'Region'
        PostalCode = 'PostalCode'
        Country = 'Country'
        HomePhone = 'HomePhone'
        Extension = 'Extension'
        Photo = 'Photo'
        Notes = 'Notes'
        ReportsTo = 'ReportsTo'
        PhotoPath = 'PhotoPath'

        def __init__(self, cnOrStr: (Connection | str)):
            self.cnOrStr = cnOrStr

        def insert(self, LastName: str, FirstName: str, Title: str = None, TitleOfCourtesy: str = None, BirthDate: datetime = None, HireDate: datetime = None, Address: str = None, City: str = None, Region: str = None, PostalCode: str = None, Country: str = None, HomePhone: str = None, Extension: str = None, Photo: bytes = None, Notes: str = None, ReportsTo: int = None, PhotoPath: str = None) -> DbCmd:
            sql = """
DECLARE
    @LastName nvarchar(40) = ?
    ,@FirstName nvarchar(20) = ?
    ,@Title nvarchar(60) = ?
    ,@TitleOfCourtesy nvarchar(50) = ?
    ,@BirthDate datetime = ?
    ,@HireDate datetime = ?
    ,@Address nvarchar(120) = ?
    ,@City nvarchar(30) = ?
    ,@Region nvarchar(30) = ?
    ,@PostalCode nvarchar(20) = ?
    ,@Country nvarchar(30) = ?
    ,@HomePhone nvarchar(48) = ?
    ,@Extension nvarchar(8) = ?
    ,@Photo image = ?
    ,@Notes ntext = ?
    ,@ReportsTo int = ?
    ,@PhotoPath nvarchar(510) = ?
;

INSERT INTO [dbo].[Employees] (
    [LastName]
    ,[FirstName]
    ,[Title]
    ,[TitleOfCourtesy]
    ,[BirthDate]
    ,[HireDate]
    ,[Address]
    ,[City]
    ,[Region]
    ,[PostalCode]
    ,[Country]
    ,[HomePhone]
    ,[Extension]
    ,[Photo]
    ,[Notes]
    ,[ReportsTo]
    ,[PhotoPath]
)
VALUES (
    @LastName
    ,@FirstName
    ,@Title
    ,@TitleOfCourtesy
    ,@BirthDate
    ,@HireDate
    ,@Address
    ,@City
    ,@Region
    ,@PostalCode
    ,@Country
    ,@HomePhone
    ,@Extension
    ,@Photo
    ,@Notes
    ,@ReportsTo
    ,@PhotoPath
);

SELECT SCOPE_IDENTITY() [scope_identity];
"""
            return DbCmd(self.cnOrStr, sql, [ LastName, FirstName, Title, TitleOfCourtesy, BirthDate, HireDate, Address, City, Region, PostalCode, Country, HomePhone, Extension, Photo, Notes, ReportsTo, PhotoPath ])

        def update(self, EmployeeID: int, LastName: str, FirstName: str, Title: str = None, TitleOfCourtesy: str = None, BirthDate: datetime = None, HireDate: datetime = None, Address: str = None, City: str = None, Region: str = None, PostalCode: str = None, Country: str = None, HomePhone: str = None, Extension: str = None, Photo: bytes = None, Notes: str = None, ReportsTo: int = None, PhotoPath: str = None) -> DbCmd:
            sql = """
DECLARE
    @EmployeeID int = ?
    ,@LastName nvarchar(40) = ?
    ,@FirstName nvarchar(20) = ?
    ,@Title nvarchar(60) = ?
    ,@TitleOfCourtesy nvarchar(50) = ?
    ,@BirthDate datetime = ?
    ,@HireDate datetime = ?
    ,@Address nvarchar(120) = ?
    ,@City nvarchar(30) = ?
    ,@Region nvarchar(30) = ?
    ,@PostalCode nvarchar(20) = ?
    ,@Country nvarchar(30) = ?
    ,@HomePhone nvarchar(48) = ?
    ,@Extension nvarchar(8) = ?
    ,@Photo image = ?
    ,@Notes ntext = ?
    ,@ReportsTo int = ?
    ,@PhotoPath nvarchar(510) = ?
;

UPDATE [dbo].[Employees] SET 
    [LastName] = @LastName
    ,[FirstName] = @FirstName
    ,[Title] = @Title
    ,[TitleOfCourtesy] = @TitleOfCourtesy
    ,[BirthDate] = @BirthDate
    ,[HireDate] = @HireDate
    ,[Address] = @Address
    ,[City] = @City
    ,[Region] = @Region
    ,[PostalCode] = @PostalCode
    ,[Country] = @Country
    ,[HomePhone] = @HomePhone
    ,[Extension] = @Extension
    ,[Photo] = @Photo
    ,[Notes] = @Notes
    ,[ReportsTo] = @ReportsTo
    ,[PhotoPath] = @PhotoPath
 WHERE
    [EmployeeID] = @EmployeeID
;

SELECT @@ROWCOUNT [rowcount];
"""
            return DbCmd(self.cnOrStr, sql, [ EmployeeID, LastName, FirstName, Title, TitleOfCourtesy, BirthDate, HireDate, Address, City, Region, PostalCode, Country, HomePhone, Extension, Photo, Notes, ReportsTo, PhotoPath ])

        def delete(self, EmployeeID: int) -> DbCmd:
            sql = """
DECLARE
    @EmployeeID int = ?
;

DELETE [dbo].[Employees]
WHERE
    [EmployeeID] = @EmployeeID
;

SELECT @@ROWCOUNT [rowcount];
"""
            return DbCmd(self.cnOrStr, sql, [ EmployeeID ])

    class Categories:
        # table
        Categories_TableName = 'Categories'
        Categories_SchemaName = 'dbo'
        Categories_QualifiedName = '[dbo].[Categories]'
        # columns
        CategoryID = 'CategoryID'
        CategoryName = 'CategoryName'
        Description = 'Description'
        Picture = 'Picture'

        def __init__(self, cnOrStr: (Connection | str)):
            self.cnOrStr = cnOrStr

        def insert(self, CategoryName: str, Description: str = None, Picture: bytes = None) -> DbCmd:
            sql = """
DECLARE
    @CategoryName nvarchar(30) = ?
    ,@Description ntext = ?
    ,@Picture image = ?
;

INSERT INTO [dbo].[Categories] (
    [CategoryName]
    ,[Description]
    ,[Picture]
)
VALUES (
    @CategoryName
    ,@Description
    ,@Picture
);

SELECT SCOPE_IDENTITY() [scope_identity];
"""
            return DbCmd(self.cnOrStr, sql, [ CategoryName, Description, Picture ])

        def update(self, CategoryID: int, CategoryName: str, Description: str = None, Picture: bytes = None) -> DbCmd:
            sql = """
DECLARE
    @CategoryID int = ?
    ,@CategoryName nvarchar(30) = ?
    ,@Description ntext = ?
    ,@Picture image = ?
;

UPDATE [dbo].[Categories] SET 
    [CategoryName] = @CategoryName
    ,[Description] = @Description
    ,[Picture] = @Picture
 WHERE
    [CategoryID] = @CategoryID
;

SELECT @@ROWCOUNT [rowcount];
"""
            return DbCmd(self.cnOrStr, sql, [ CategoryID, CategoryName, Description, Picture ])

        def delete(self, CategoryID: int) -> DbCmd:
            sql = """
DECLARE
    @CategoryID int = ?
;

DELETE [dbo].[Categories]
WHERE
    [CategoryID] = @CategoryID
;

SELECT @@ROWCOUNT [rowcount];
"""
            return DbCmd(self.cnOrStr, sql, [ CategoryID ])

    class Customers:
        # table
        Customers_TableName = 'Customers'
        Customers_SchemaName = 'dbo'
        Customers_QualifiedName = '[dbo].[Customers]'
        # columns
        CustomerID = 'CustomerID'
        CompanyName = 'CompanyName'
        ContactName = 'ContactName'
        ContactTitle = 'ContactTitle'
        Address = 'Address'
        City = 'City'
        Region = 'Region'
        PostalCode = 'PostalCode'
        Country = 'Country'
        Phone = 'Phone'
        Fax = 'Fax'

        def __init__(self, cnOrStr: (Connection | str)):
            self.cnOrStr = cnOrStr

        def insert(self, CustomerID: str, CompanyName: str, ContactName: str = None, ContactTitle: str = None, Address: str = None, City: str = None, Region: str = None, PostalCode: str = None, Country: str = None, Phone: str = None, Fax: str = None) -> DbCmd:
            sql = """
DECLARE
    @CustomerID nchar(10) = ?
    ,@CompanyName nvarchar(80) = ?
    ,@ContactName nvarchar(60) = ?
    ,@ContactTitle nvarchar(60) = ?
    ,@Address nvarchar(120) = ?
    ,@City nvarchar(30) = ?
    ,@Region nvarchar(30) = ?
    ,@PostalCode nvarchar(20) = ?
    ,@Country nvarchar(30) = ?
    ,@Phone nvarchar(48) = ?
    ,@Fax nvarchar(48) = ?
;

INSERT INTO [dbo].[Customers] (
    [CustomerID]
    ,[CompanyName]
    ,[ContactName]
    ,[ContactTitle]
    ,[Address]
    ,[City]
    ,[Region]
    ,[PostalCode]
    ,[Country]
    ,[Phone]
    ,[Fax]
)
VALUES (
    @CustomerID
    ,@CompanyName
    ,@ContactName
    ,@ContactTitle
    ,@Address
    ,@City
    ,@Region
    ,@PostalCode
    ,@Country
    ,@Phone
    ,@Fax
);
"""
            return DbCmd(self.cnOrStr, sql, [ CustomerID, CompanyName, ContactName, ContactTitle, Address, City, Region, PostalCode, Country, Phone, Fax ])

        def update(self, CustomerID: str, CompanyName: str, ContactName: str = None, ContactTitle: str = None, Address: str = None, City: str = None, Region: str = None, PostalCode: str = None, Country: str = None, Phone: str = None, Fax: str = None) -> DbCmd:
            sql = """
DECLARE
    @CustomerID nchar(10) = ?
    ,@CompanyName nvarchar(80) = ?
    ,@ContactName nvarchar(60) = ?
    ,@ContactTitle nvarchar(60) = ?
    ,@Address nvarchar(120) = ?
    ,@City nvarchar(30) = ?
    ,@Region nvarchar(30) = ?
    ,@PostalCode nvarchar(20) = ?
    ,@Country nvarchar(30) = ?
    ,@Phone nvarchar(48) = ?
    ,@Fax nvarchar(48) = ?
;

UPDATE [dbo].[Customers] SET 
    [CompanyName] = @CompanyName
    ,[ContactName] = @ContactName
    ,[ContactTitle] = @ContactTitle
    ,[Address] = @Address
    ,[City] = @City
    ,[Region] = @Region
    ,[PostalCode] = @PostalCode
    ,[Country] = @Country
    ,[Phone] = @Phone
    ,[Fax] = @Fax
 WHERE
    [CustomerID] = @CustomerID
;

SELECT @@ROWCOUNT [rowcount];
"""
            return DbCmd(self.cnOrStr, sql, [ CustomerID, CompanyName, ContactName, ContactTitle, Address, City, Region, PostalCode, Country, Phone, Fax ])

        def delete(self, CustomerID: str) -> DbCmd:
            sql = """
DECLARE
    @CustomerID nchar(10) = ?
;

DELETE [dbo].[Customers]
WHERE
    [CustomerID] = @CustomerID
;

SELECT @@ROWCOUNT [rowcount];
"""
            return DbCmd(self.cnOrStr, sql, [ CustomerID ])

    class Shippers:
        # table
        Shippers_TableName = 'Shippers'
        Shippers_SchemaName = 'dbo'
        Shippers_QualifiedName = '[dbo].[Shippers]'
        # columns
        ShipperID = 'ShipperID'
        CompanyName = 'CompanyName'
        Phone = 'Phone'

        def __init__(self, cnOrStr: (Connection | str)):
            self.cnOrStr = cnOrStr

        def insert(self, CompanyName: str, Phone: str = None) -> DbCmd:
            sql = """
DECLARE
    @CompanyName nvarchar(80) = ?
    ,@Phone nvarchar(48) = ?
;

INSERT INTO [dbo].[Shippers] (
    [CompanyName]
    ,[Phone]
)
VALUES (
    @CompanyName
    ,@Phone
);

SELECT SCOPE_IDENTITY() [scope_identity];
"""
            return DbCmd(self.cnOrStr, sql, [ CompanyName, Phone ])

        def update(self, ShipperID: int, CompanyName: str, Phone: str = None) -> DbCmd:
            sql = """
DECLARE
    @ShipperID int = ?
    ,@CompanyName nvarchar(80) = ?
    ,@Phone nvarchar(48) = ?
;

UPDATE [dbo].[Shippers] SET 
    [CompanyName] = @CompanyName
    ,[Phone] = @Phone
 WHERE
    [ShipperID] = @ShipperID
;

SELECT @@ROWCOUNT [rowcount];
"""
            return DbCmd(self.cnOrStr, sql, [ ShipperID, CompanyName, Phone ])

        def delete(self, ShipperID: int) -> DbCmd:
            sql = """
DECLARE
    @ShipperID int = ?
;

DELETE [dbo].[Shippers]
WHERE
    [ShipperID] = @ShipperID
;

SELECT @@ROWCOUNT [rowcount];
"""
            return DbCmd(self.cnOrStr, sql, [ ShipperID ])

    class Suppliers:
        # table
        Suppliers_TableName = 'Suppliers'
        Suppliers_SchemaName = 'dbo'
        Suppliers_QualifiedName = '[dbo].[Suppliers]'
        # columns
        SupplierID = 'SupplierID'
        CompanyName = 'CompanyName'
        ContactName = 'ContactName'
        ContactTitle = 'ContactTitle'
        Address = 'Address'
        City = 'City'
        Region = 'Region'
        PostalCode = 'PostalCode'
        Country = 'Country'
        Phone = 'Phone'
        Fax = 'Fax'
        HomePage = 'HomePage'

        def __init__(self, cnOrStr: (Connection | str)):
            self.cnOrStr = cnOrStr

        def insert(self, CompanyName: str, ContactName: str = None, ContactTitle: str = None, Address: str = None, City: str = None, Region: str = None, PostalCode: str = None, Country: str = None, Phone: str = None, Fax: str = None, HomePage: str = None) -> DbCmd:
            sql = """
DECLARE
    @CompanyName nvarchar(80) = ?
    ,@ContactName nvarchar(60) = ?
    ,@ContactTitle nvarchar(60) = ?
    ,@Address nvarchar(120) = ?
    ,@City nvarchar(30) = ?
    ,@Region nvarchar(30) = ?
    ,@PostalCode nvarchar(20) = ?
    ,@Country nvarchar(30) = ?
    ,@Phone nvarchar(48) = ?
    ,@Fax nvarchar(48) = ?
    ,@HomePage ntext = ?
;

INSERT INTO [dbo].[Suppliers] (
    [CompanyName]
    ,[ContactName]
    ,[ContactTitle]
    ,[Address]
    ,[City]
    ,[Region]
    ,[PostalCode]
    ,[Country]
    ,[Phone]
    ,[Fax]
    ,[HomePage]
)
VALUES (
    @CompanyName
    ,@ContactName
    ,@ContactTitle
    ,@Address
    ,@City
    ,@Region
    ,@PostalCode
    ,@Country
    ,@Phone
    ,@Fax
    ,@HomePage
);

SELECT SCOPE_IDENTITY() [scope_identity];
"""
            return DbCmd(self.cnOrStr, sql, [ CompanyName, ContactName, ContactTitle, Address, City, Region, PostalCode, Country, Phone, Fax, HomePage ])

        def update(self, SupplierID: int, CompanyName: str, ContactName: str = None, ContactTitle: str = None, Address: str = None, City: str = None, Region: str = None, PostalCode: str = None, Country: str = None, Phone: str = None, Fax: str = None, HomePage: str = None) -> DbCmd:
            sql = """
DECLARE
    @SupplierID int = ?
    ,@CompanyName nvarchar(80) = ?
    ,@ContactName nvarchar(60) = ?
    ,@ContactTitle nvarchar(60) = ?
    ,@Address nvarchar(120) = ?
    ,@City nvarchar(30) = ?
    ,@Region nvarchar(30) = ?
    ,@PostalCode nvarchar(20) = ?
    ,@Country nvarchar(30) = ?
    ,@Phone nvarchar(48) = ?
    ,@Fax nvarchar(48) = ?
    ,@HomePage ntext = ?
;

UPDATE [dbo].[Suppliers] SET 
    [CompanyName] = @CompanyName
    ,[ContactName] = @ContactName
    ,[ContactTitle] = @ContactTitle
    ,[Address] = @Address
    ,[City] = @City
    ,[Region] = @Region
    ,[PostalCode] = @PostalCode
    ,[Country] = @Country
    ,[Phone] = @Phone
    ,[Fax] = @Fax
    ,[HomePage] = @HomePage
 WHERE
    [SupplierID] = @SupplierID
;

SELECT @@ROWCOUNT [rowcount];
"""
            return DbCmd(self.cnOrStr, sql, [ SupplierID, CompanyName, ContactName, ContactTitle, Address, City, Region, PostalCode, Country, Phone, Fax, HomePage ])

        def delete(self, SupplierID: int) -> DbCmd:
            sql = """
DECLARE
    @SupplierID int = ?
;

DELETE [dbo].[Suppliers]
WHERE
    [SupplierID] = @SupplierID
;

SELECT @@ROWCOUNT [rowcount];
"""
            return DbCmd(self.cnOrStr, sql, [ SupplierID ])

    class Orders:
        # table
        Orders_TableName = 'Orders'
        Orders_SchemaName = 'dbo'
        Orders_QualifiedName = '[dbo].[Orders]'
        # columns
        OrderID = 'OrderID'
        CustomerID = 'CustomerID'
        EmployeeID = 'EmployeeID'
        OrderDate = 'OrderDate'
        RequiredDate = 'RequiredDate'
        ShippedDate = 'ShippedDate'
        ShipVia = 'ShipVia'
        Freight = 'Freight'
        ShipName = 'ShipName'
        ShipAddress = 'ShipAddress'
        ShipCity = 'ShipCity'
        ShipRegion = 'ShipRegion'
        ShipPostalCode = 'ShipPostalCode'
        ShipCountry = 'ShipCountry'

        def __init__(self, cnOrStr: (Connection | str)):
            self.cnOrStr = cnOrStr

        def insert(self, CustomerID: str = None, EmployeeID: int = None, OrderDate: datetime = None, RequiredDate: datetime = None, ShippedDate: datetime = None, ShipVia: int = None, Freight: float = None, ShipName: str = None, ShipAddress: str = None, ShipCity: str = None, ShipRegion: str = None, ShipPostalCode: str = None, ShipCountry: str = None) -> DbCmd:
            sql = """
DECLARE
    @CustomerID nchar(10) = ?
    ,@EmployeeID int = ?
    ,@OrderDate datetime = ?
    ,@RequiredDate datetime = ?
    ,@ShippedDate datetime = ?
    ,@ShipVia int = ?
    ,@Freight money(19, 4) = ?
    ,@ShipName nvarchar(80) = ?
    ,@ShipAddress nvarchar(120) = ?
    ,@ShipCity nvarchar(30) = ?
    ,@ShipRegion nvarchar(30) = ?
    ,@ShipPostalCode nvarchar(20) = ?
    ,@ShipCountry nvarchar(30) = ?
;

INSERT INTO [dbo].[Orders] (
    [CustomerID]
    ,[EmployeeID]
    ,[OrderDate]
    ,[RequiredDate]
    ,[ShippedDate]
    ,[ShipVia]
    ,[Freight]
    ,[ShipName]
    ,[ShipAddress]
    ,[ShipCity]
    ,[ShipRegion]
    ,[ShipPostalCode]
    ,[ShipCountry]
)
VALUES (
    @CustomerID
    ,@EmployeeID
    ,@OrderDate
    ,@RequiredDate
    ,@ShippedDate
    ,@ShipVia
    ,@Freight
    ,@ShipName
    ,@ShipAddress
    ,@ShipCity
    ,@ShipRegion
    ,@ShipPostalCode
    ,@ShipCountry
);

SELECT SCOPE_IDENTITY() [scope_identity];
"""
            return DbCmd(self.cnOrStr, sql, [ CustomerID, EmployeeID, OrderDate, RequiredDate, ShippedDate, ShipVia, Freight, ShipName, ShipAddress, ShipCity, ShipRegion, ShipPostalCode, ShipCountry ])

        def update(self, OrderID: int, CustomerID: str = None, EmployeeID: int = None, OrderDate: datetime = None, RequiredDate: datetime = None, ShippedDate: datetime = None, ShipVia: int = None, Freight: float = None, ShipName: str = None, ShipAddress: str = None, ShipCity: str = None, ShipRegion: str = None, ShipPostalCode: str = None, ShipCountry: str = None) -> DbCmd:
            sql = """
DECLARE
    @OrderID int = ?
    ,@CustomerID nchar(10) = ?
    ,@EmployeeID int = ?
    ,@OrderDate datetime = ?
    ,@RequiredDate datetime = ?
    ,@ShippedDate datetime = ?
    ,@ShipVia int = ?
    ,@Freight money(19, 4) = ?
    ,@ShipName nvarchar(80) = ?
    ,@ShipAddress nvarchar(120) = ?
    ,@ShipCity nvarchar(30) = ?
    ,@ShipRegion nvarchar(30) = ?
    ,@ShipPostalCode nvarchar(20) = ?
    ,@ShipCountry nvarchar(30) = ?
;

UPDATE [dbo].[Orders] SET 
    [CustomerID] = @CustomerID
    ,[EmployeeID] = @EmployeeID
    ,[OrderDate] = @OrderDate
    ,[RequiredDate] = @RequiredDate
    ,[ShippedDate] = @ShippedDate
    ,[ShipVia] = @ShipVia
    ,[Freight] = @Freight
    ,[ShipName] = @ShipName
    ,[ShipAddress] = @ShipAddress
    ,[ShipCity] = @ShipCity
    ,[ShipRegion] = @ShipRegion
    ,[ShipPostalCode] = @ShipPostalCode
    ,[ShipCountry] = @ShipCountry
 WHERE
    [OrderID] = @OrderID
;

SELECT @@ROWCOUNT [rowcount];
"""
            return DbCmd(self.cnOrStr, sql, [ OrderID, CustomerID, EmployeeID, OrderDate, RequiredDate, ShippedDate, ShipVia, Freight, ShipName, ShipAddress, ShipCity, ShipRegion, ShipPostalCode, ShipCountry ])

        def delete(self, OrderID: int) -> DbCmd:
            sql = """
DECLARE
    @OrderID int = ?
;

DELETE [dbo].[Orders]
WHERE
    [OrderID] = @OrderID
;

SELECT @@ROWCOUNT [rowcount];
"""
            return DbCmd(self.cnOrStr, sql, [ OrderID ])

    class Products:
        # table
        Products_TableName = 'Products'
        Products_SchemaName = 'dbo'
        Products_QualifiedName = '[dbo].[Products]'
        # columns
        ProductID = 'ProductID'
        ProductName = 'ProductName'
        SupplierID = 'SupplierID'
        CategoryID = 'CategoryID'
        QuantityPerUnit = 'QuantityPerUnit'
        UnitPrice = 'UnitPrice'
        UnitsInStock = 'UnitsInStock'
        UnitsOnOrder = 'UnitsOnOrder'
        ReorderLevel = 'ReorderLevel'
        Discontinued = 'Discontinued'

        def __init__(self, cnOrStr: (Connection | str)):
            self.cnOrStr = cnOrStr

        def insert(self, ProductName: str, Discontinued: int, SupplierID: int = None, CategoryID: int = None, QuantityPerUnit: str = None, UnitPrice: float = None, UnitsInStock: int = None, UnitsOnOrder: int = None, ReorderLevel: int = None) -> DbCmd:
            sql = """
DECLARE
    @ProductName nvarchar(80) = ?
    ,@SupplierID int = ?
    ,@CategoryID int = ?
    ,@QuantityPerUnit nvarchar(40) = ?
    ,@UnitPrice money(19, 4) = ?
    ,@UnitsInStock smallint = ?
    ,@UnitsOnOrder smallint = ?
    ,@ReorderLevel smallint = ?
    ,@Discontinued bit = ?
;

INSERT INTO [dbo].[Products] (
    [ProductName]
    ,[SupplierID]
    ,[CategoryID]
    ,[QuantityPerUnit]
    ,[UnitPrice]
    ,[UnitsInStock]
    ,[UnitsOnOrder]
    ,[ReorderLevel]
    ,[Discontinued]
)
VALUES (
    @ProductName
    ,@SupplierID
    ,@CategoryID
    ,@QuantityPerUnit
    ,@UnitPrice
    ,@UnitsInStock
    ,@UnitsOnOrder
    ,@ReorderLevel
    ,@Discontinued
);

SELECT SCOPE_IDENTITY() [scope_identity];
"""
            return DbCmd(self.cnOrStr, sql, [ ProductName, SupplierID, CategoryID, QuantityPerUnit, UnitPrice, UnitsInStock, UnitsOnOrder, ReorderLevel, Discontinued ])

        def update(self, ProductID: int, ProductName: str, Discontinued: int, SupplierID: int = None, CategoryID: int = None, QuantityPerUnit: str = None, UnitPrice: float = None, UnitsInStock: int = None, UnitsOnOrder: int = None, ReorderLevel: int = None) -> DbCmd:
            sql = """
DECLARE
    @ProductID int = ?
    ,@ProductName nvarchar(80) = ?
    ,@SupplierID int = ?
    ,@CategoryID int = ?
    ,@QuantityPerUnit nvarchar(40) = ?
    ,@UnitPrice money(19, 4) = ?
    ,@UnitsInStock smallint = ?
    ,@UnitsOnOrder smallint = ?
    ,@ReorderLevel smallint = ?
    ,@Discontinued bit = ?
;

UPDATE [dbo].[Products] SET 
    [ProductName] = @ProductName
    ,[SupplierID] = @SupplierID
    ,[CategoryID] = @CategoryID
    ,[QuantityPerUnit] = @QuantityPerUnit
    ,[UnitPrice] = @UnitPrice
    ,[UnitsInStock] = @UnitsInStock
    ,[UnitsOnOrder] = @UnitsOnOrder
    ,[ReorderLevel] = @ReorderLevel
    ,[Discontinued] = @Discontinued
 WHERE
    [ProductID] = @ProductID
;

SELECT @@ROWCOUNT [rowcount];
"""
            return DbCmd(self.cnOrStr, sql, [ ProductID, ProductName, SupplierID, CategoryID, QuantityPerUnit, UnitPrice, UnitsInStock, UnitsOnOrder, ReorderLevel, Discontinued ])

        def delete(self, ProductID: int) -> DbCmd:
            sql = """
DECLARE
    @ProductID int = ?
;

DELETE [dbo].[Products]
WHERE
    [ProductID] = @ProductID
;

SELECT @@ROWCOUNT [rowcount];
"""
            return DbCmd(self.cnOrStr, sql, [ ProductID ])

    class Order_Details:
        # table
        Order_Details_TableName = 'Order Details'
        Order_Details_SchemaName = 'dbo'
        Order_Details_QualifiedName = '[dbo].[Order_Details]'
        # columns
        OrderID = 'OrderID'
        ProductID = 'ProductID'
        UnitPrice = 'UnitPrice'
        Quantity = 'Quantity'
        Discount = 'Discount'

        def __init__(self, cnOrStr: (Connection | str)):
            self.cnOrStr = cnOrStr

        def insert(self, OrderID: int, ProductID: int, UnitPrice: float, Quantity: int, Discount: float) -> DbCmd:
            sql = """
DECLARE
    @OrderID int = ?
    ,@ProductID int = ?
    ,@UnitPrice money(19, 4) = ?
    ,@Quantity smallint = ?
    ,@Discount real = ?
;

INSERT INTO [dbo].[Order Details] (
    [OrderID]
    ,[ProductID]
    ,[UnitPrice]
    ,[Quantity]
    ,[Discount]
)
VALUES (
    @OrderID
    ,@ProductID
    ,@UnitPrice
    ,@Quantity
    ,@Discount
);
"""
            return DbCmd(self.cnOrStr, sql, [ OrderID, ProductID, UnitPrice, Quantity, Discount ])

        def update(self, OrderID: int, ProductID: int, UnitPrice: float, Quantity: int, Discount: float) -> DbCmd:
            sql = """
DECLARE
    @OrderID int = ?
    ,@ProductID int = ?
    ,@UnitPrice money(19, 4) = ?
    ,@Quantity smallint = ?
    ,@Discount real = ?
;

UPDATE [dbo].[Order Details] SET 
    [UnitPrice] = @UnitPrice
    ,[Quantity] = @Quantity
    ,[Discount] = @Discount
 WHERE
    [OrderID] = @OrderID
    ,[ProductID] = @ProductID
;

SELECT @@ROWCOUNT [rowcount];
"""
            return DbCmd(self.cnOrStr, sql, [ OrderID, ProductID, UnitPrice, Quantity, Discount ])

        def delete(self, OrderID: int, ProductID: int) -> DbCmd:
            sql = """
DECLARE
    @OrderID int = ?
    ,@ProductID int = ?
;

DELETE [dbo].[Order Details]
WHERE
    [OrderID] = @OrderID
    ,[ProductID] = @ProductID
;

SELECT @@ROWCOUNT [rowcount];
"""
            return DbCmd(self.cnOrStr, sql, [ OrderID, ProductID ])

    class CustomerCustomerDemo:
        # table
        CustomerCustomerDemo_TableName = 'CustomerCustomerDemo'
        CustomerCustomerDemo_SchemaName = 'dbo'
        CustomerCustomerDemo_QualifiedName = '[dbo].[CustomerCustomerDemo]'
        # columns
        CustomerID = 'CustomerID'
        CustomerTypeID = 'CustomerTypeID'

        def __init__(self, cnOrStr: (Connection | str)):
            self.cnOrStr = cnOrStr

        def insert(self, CustomerID: str, CustomerTypeID: str) -> DbCmd:
            sql = """
DECLARE
    @CustomerID nchar(10) = ?
    ,@CustomerTypeID nchar(20) = ?
;

INSERT INTO [dbo].[CustomerCustomerDemo] (
    [CustomerID]
    ,[CustomerTypeID]
)
VALUES (
    @CustomerID
    ,@CustomerTypeID
);
"""
            return DbCmd(self.cnOrStr, sql, [ CustomerID, CustomerTypeID ])

        def update(self, CustomerID: str, CustomerTypeID: str) -> DbCmd:
            sql = """
DECLARE
    @CustomerID nchar(10) = ?
    ,@CustomerTypeID nchar(20) = ?
;

UPDATE [dbo].[CustomerCustomerDemo] SET 
 WHERE
    [CustomerID] = @CustomerID
    ,[CustomerTypeID] = @CustomerTypeID
;

SELECT @@ROWCOUNT [rowcount];
"""
            return DbCmd(self.cnOrStr, sql, [ CustomerID, CustomerTypeID ])

        def delete(self, CustomerID: str, CustomerTypeID: str) -> DbCmd:
            sql = """
DECLARE
    @CustomerID nchar(10) = ?
    ,@CustomerTypeID nchar(20) = ?
;

DELETE [dbo].[CustomerCustomerDemo]
WHERE
    [CustomerID] = @CustomerID
    ,[CustomerTypeID] = @CustomerTypeID
;

SELECT @@ROWCOUNT [rowcount];
"""
            return DbCmd(self.cnOrStr, sql, [ CustomerID, CustomerTypeID ])

    class CustomerDemographics:
        # table
        CustomerDemographics_TableName = 'CustomerDemographics'
        CustomerDemographics_SchemaName = 'dbo'
        CustomerDemographics_QualifiedName = '[dbo].[CustomerDemographics]'
        # columns
        CustomerTypeID = 'CustomerTypeID'
        CustomerDesc = 'CustomerDesc'

        def __init__(self, cnOrStr: (Connection | str)):
            self.cnOrStr = cnOrStr

        def insert(self, CustomerTypeID: str, CustomerDesc: str = None) -> DbCmd:
            sql = """
DECLARE
    @CustomerTypeID nchar(20) = ?
    ,@CustomerDesc ntext = ?
;

INSERT INTO [dbo].[CustomerDemographics] (
    [CustomerTypeID]
    ,[CustomerDesc]
)
VALUES (
    @CustomerTypeID
    ,@CustomerDesc
);
"""
            return DbCmd(self.cnOrStr, sql, [ CustomerTypeID, CustomerDesc ])

        def update(self, CustomerTypeID: str, CustomerDesc: str = None) -> DbCmd:
            sql = """
DECLARE
    @CustomerTypeID nchar(20) = ?
    ,@CustomerDesc ntext = ?
;

UPDATE [dbo].[CustomerDemographics] SET 
    [CustomerDesc] = @CustomerDesc
 WHERE
    [CustomerTypeID] = @CustomerTypeID
;

SELECT @@ROWCOUNT [rowcount];
"""
            return DbCmd(self.cnOrStr, sql, [ CustomerTypeID, CustomerDesc ])

        def delete(self, CustomerTypeID: str) -> DbCmd:
            sql = """
DECLARE
    @CustomerTypeID nchar(20) = ?
;

DELETE [dbo].[CustomerDemographics]
WHERE
    [CustomerTypeID] = @CustomerTypeID
;

SELECT @@ROWCOUNT [rowcount];
"""
            return DbCmd(self.cnOrStr, sql, [ CustomerTypeID ])

    class Region:
        # table
        Region_TableName = 'Region'
        Region_SchemaName = 'dbo'
        Region_QualifiedName = '[dbo].[Region]'
        # columns
        RegionID = 'RegionID'
        RegionDescription = 'RegionDescription'

        def __init__(self, cnOrStr: (Connection | str)):
            self.cnOrStr = cnOrStr

        def insert(self, RegionID: int, RegionDescription: str) -> DbCmd:
            sql = """
DECLARE
    @RegionID int = ?
    ,@RegionDescription nchar(100) = ?
;

INSERT INTO [dbo].[Region] (
    [RegionID]
    ,[RegionDescription]
)
VALUES (
    @RegionID
    ,@RegionDescription
);
"""
            return DbCmd(self.cnOrStr, sql, [ RegionID, RegionDescription ])

        def update(self, RegionID: int, RegionDescription: str) -> DbCmd:
            sql = """
DECLARE
    @RegionID int = ?
    ,@RegionDescription nchar(100) = ?
;

UPDATE [dbo].[Region] SET 
    [RegionDescription] = @RegionDescription
 WHERE
    [RegionID] = @RegionID
;

SELECT @@ROWCOUNT [rowcount];
"""
            return DbCmd(self.cnOrStr, sql, [ RegionID, RegionDescription ])

        def delete(self, RegionID: int) -> DbCmd:
            sql = """
DECLARE
    @RegionID int = ?
;

DELETE [dbo].[Region]
WHERE
    [RegionID] = @RegionID
;

SELECT @@ROWCOUNT [rowcount];
"""
            return DbCmd(self.cnOrStr, sql, [ RegionID ])

    class Territories:
        # table
        Territories_TableName = 'Territories'
        Territories_SchemaName = 'dbo'
        Territories_QualifiedName = '[dbo].[Territories]'
        # columns
        TerritoryID = 'TerritoryID'
        TerritoryDescription = 'TerritoryDescription'
        RegionID = 'RegionID'

        def __init__(self, cnOrStr: (Connection | str)):
            self.cnOrStr = cnOrStr

        def insert(self, TerritoryID: str, TerritoryDescription: str, RegionID: int) -> DbCmd:
            sql = """
DECLARE
    @TerritoryID nvarchar(40) = ?
    ,@TerritoryDescription nchar(100) = ?
    ,@RegionID int = ?
;

INSERT INTO [dbo].[Territories] (
    [TerritoryID]
    ,[TerritoryDescription]
    ,[RegionID]
)
VALUES (
    @TerritoryID
    ,@TerritoryDescription
    ,@RegionID
);
"""
            return DbCmd(self.cnOrStr, sql, [ TerritoryID, TerritoryDescription, RegionID ])

        def update(self, TerritoryID: str, TerritoryDescription: str, RegionID: int) -> DbCmd:
            sql = """
DECLARE
    @TerritoryID nvarchar(40) = ?
    ,@TerritoryDescription nchar(100) = ?
    ,@RegionID int = ?
;

UPDATE [dbo].[Territories] SET 
    [TerritoryDescription] = @TerritoryDescription
    ,[RegionID] = @RegionID
 WHERE
    [TerritoryID] = @TerritoryID
;

SELECT @@ROWCOUNT [rowcount];
"""
            return DbCmd(self.cnOrStr, sql, [ TerritoryID, TerritoryDescription, RegionID ])

        def delete(self, TerritoryID: str) -> DbCmd:
            sql = """
DECLARE
    @TerritoryID nvarchar(40) = ?
;

DELETE [dbo].[Territories]
WHERE
    [TerritoryID] = @TerritoryID
;

SELECT @@ROWCOUNT [rowcount];
"""
            return DbCmd(self.cnOrStr, sql, [ TerritoryID ])

    class EmployeeTerritories:
        # table
        EmployeeTerritories_TableName = 'EmployeeTerritories'
        EmployeeTerritories_SchemaName = 'dbo'
        EmployeeTerritories_QualifiedName = '[dbo].[EmployeeTerritories]'
        # columns
        EmployeeID = 'EmployeeID'
        TerritoryID = 'TerritoryID'

        def __init__(self, cnOrStr: (Connection | str)):
            self.cnOrStr = cnOrStr

        def insert(self, EmployeeID: int, TerritoryID: str) -> DbCmd:
            sql = """
DECLARE
    @EmployeeID int = ?
    ,@TerritoryID nvarchar(40) = ?
;

INSERT INTO [dbo].[EmployeeTerritories] (
    [EmployeeID]
    ,[TerritoryID]
)
VALUES (
    @EmployeeID
    ,@TerritoryID
);
"""
            return DbCmd(self.cnOrStr, sql, [ EmployeeID, TerritoryID ])

        def update(self, EmployeeID: int, TerritoryID: str) -> DbCmd:
            sql = """
DECLARE
    @EmployeeID int = ?
    ,@TerritoryID nvarchar(40) = ?
;

UPDATE [dbo].[EmployeeTerritories] SET 
 WHERE
    [EmployeeID] = @EmployeeID
    ,[TerritoryID] = @TerritoryID
;

SELECT @@ROWCOUNT [rowcount];
"""
            return DbCmd(self.cnOrStr, sql, [ EmployeeID, TerritoryID ])

        def delete(self, EmployeeID: int, TerritoryID: str) -> DbCmd:
            sql = """
DECLARE
    @EmployeeID int = ?
    ,@TerritoryID nvarchar(40) = ?
;

DELETE [dbo].[EmployeeTerritories]
WHERE
    [EmployeeID] = @EmployeeID
    ,[TerritoryID] = @TerritoryID
;

SELECT @@ROWCOUNT [rowcount];
"""
            return DbCmd(self.cnOrStr, sql, [ EmployeeID, TerritoryID ])

