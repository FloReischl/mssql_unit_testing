CREATE OR ALTER PROCEDURE mrtest.usp_restore_table
    @schema_name NVARCHAR(256)
    ,@table_name NVARCHAR(256)
AS

DECLARE @hidden_name NVARCHAR(256) = @table_name + '_hidden_mrtest';

DECLARE @full_table_name NVARCHAR(512)  = QUOTENAME(@schema_name) + '.' + QUOTENAME(@table_name);
DECLARE @full_hidden_name NVARCHAR(512) = QUOTENAME(@schema_name) + '.' + QUOTENAME(@hidden_name);

-- check current mock table exists
IF NOT EXISTS (SELECT * FROM sys.tables WHERE object_id = OBJECT_ID(@full_table_name))
    THROW 50000, 'Mock table does not exist in schema. Please verify and fix the database schema.', 1;

-- check recovery table exists
IF NOT EXISTS (SELECT * FROM sys.tables WHERE object_id = OBJECT_ID(@full_hidden_name))
    THROW 50000, 'Hidden recovery table already exists. Please verify and fix the database schema.', 1;

-- drop mock table
EXECUTE ('DROP TABLE ' + @full_table_name);

-- restore original table
EXECUTE sp_rename @objname = @full_hidden_name, @newname = @table_name, @objtype = 'OBJECT';
GO

--EXECUTE mrtest.usp_restore_table @schema_name = 'examples', @table_name = 'Order Details';
