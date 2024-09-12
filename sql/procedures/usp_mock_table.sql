CREATE OR ALTER PROCEDURE mrtest.usp_mock_table
    @schema_name NVARCHAR(256)
    ,@table_name NVARCHAR(256)
AS

DECLARE @hidden_name NVARCHAR(256) = @table_name + '_hidden_cb751972';
DECLARE @mock_name NVARCHAR(256) = @table_name + '_mock_cb751972';

DECLARE @full_table_name NVARCHAR(512)  = QUOTENAME(@schema_name) + '.' + QUOTENAME(@table_name);
DECLARE @full_hidden_name NVARCHAR(512) = QUOTENAME(@schema_name) + '.' + QUOTENAME(@hidden_name);
DECLARE @full_mock_name NVARCHAR(512)   = QUOTENAME(@schema_name) + '.' + QUOTENAME(@mock_name)

IF NOT EXISTS (SELECT * FROM sys.tables WHERE object_id = OBJECT_ID(@full_table_name))
    THROW 50000, 'Table does not exist in schema.', 1;

IF EXISTS (SELECT * FROM sys.tables WHERE object_id = OBJECT_ID(@full_hidden_name))
    THROW 50000, 'Hidden recovery table already exists. Please verify and fix your database.', 1;

IF EXISTS (SELECT * FROM sys.tables WHERE object_id = OBJECT_ID(@full_mock_name))
    THROW 50000, 'Mock table already exists. Please verify and fix your database.', 1;

DECLARE @sql NVARCHAR(MAX);

SET @sql = CONCAT('SELECT TOP(0) * ',
' INTO ', @full_mock_name, 
' FROM ', @full_table_name);
EXECUTE (@sql);

EXECUTE sp_rename @objname = @full_table_name, @newname = @hidden_name, @objtype = 'OBJECT';

EXECUTE sp_rename @objname = @full_mock_name, @newname = @table_name, @objtype = 'OBJECT';

-- SELECT * FROM examples.[Order Details]

GO
EXECUTE mrtest.usp_mock_table
    @schema_name = 'examples'
    ,@table_name = 'Order Details';

-- select * FROM sys.tables WHERE object_id = NULL
