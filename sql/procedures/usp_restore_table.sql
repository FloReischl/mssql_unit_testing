CREATE OR ALTER PROCEDURE mrtest.usp_restore_table
    @schema_name NVARCHAR(256)
    ,@table_name NVARCHAR(256)
AS

DECLARE @hidden_name NVARCHAR(256) = @table_name + '_hidden_cb751972';

DECLARE @full_table_name NVARCHAR(512)  = QUOTENAME(@schema_name) + '.' + QUOTENAME(@table_name);
DECLARE @full_hidden_name NVARCHAR(512) = QUOTENAME(@schema_name) + '.' + QUOTENAME(@hidden_name);

IF NOT EXISTS (SELECT * FROM sys.tables WHERE object_id = OBJECT_ID(@full_table_name))
    THROW 50000, 'Mock table does not exist in schema. Please verify and fix the database schema.', 1;

IF NOT EXISTS (SELECT * FROM sys.tables WHERE object_id = OBJECT_ID(@full_hidden_name))
    THROW 50000, 'Hidden recovery table already exists. Please verify and fix the database schema.', 1;

EXECUTE ('DROP TABLE ' + @full_table_name);

EXECUTE sp_rename @objname = @full_hidden_name, @newname = @table_name, @objtype = 'OBJECT';
