/****** Object:  StoredProcedure [mrtest].[usp_mock_table]    Script Date: 2024-09-19 16:31:02 ******/
DROP PROCEDURE [mrtest].[usp_mock_table]
GO
/****** Object:  StoredProcedure [mrtest].[usp_mock_table]    Script Date: 2024-09-19 16:31:02 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE   PROCEDURE [mrtest].[usp_mock_table]
    @schema_name NVARCHAR(256)
    ,@table_name NVARCHAR(256)
AS
SET NOCOUNT ON;

DECLARE @hidden_name NVARCHAR(256) = @table_name + '_HIDDEN_MRTEST';
DECLARE @mock_name NVARCHAR(256) = @table_name + '_MOCK_MRTEST';

DECLARE @full_table_name NVARCHAR(512)  = QUOTENAME(@schema_name) + '.' + QUOTENAME(@table_name);
DECLARE @full_hidden_name NVARCHAR(512) = QUOTENAME(@schema_name) + '.' + QUOTENAME(@hidden_name);
DECLARE @full_mock_name NVARCHAR(512)   = QUOTENAME(@schema_name) + '.' + QUOTENAME(@mock_name)

DECLARE @sql NVARCHAR(MAX);

-- check original table exists
IF NOT EXISTS (SELECT * FROM sys.tables WHERE object_id = OBJECT_ID(@full_table_name))
    THROW 50000, 'Table does not exist in schema.', 1;

-- check hidden table name already exists
IF EXISTS (SELECT * FROM sys.tables WHERE object_id = OBJECT_ID(@full_hidden_name))
    THROW 50000, 'Hidden recovery table already exists. Please verify and fix your database.', 1;

-- check intermediate mock table name already exists
IF EXISTS (SELECT * FROM sys.tables WHERE object_id = OBJECT_ID(@full_mock_name))
    THROW 50000, 'Mock table already exists. Please verify and fix your database.', 1;

-- create mock table from original table
SET @sql = CONCAT('SELECT TOP(0) * ',
' INTO ', @full_mock_name, 
' FROM ', @full_table_name);
EXECUTE (@sql);

-- rename original to hidden
EXECUTE sp_rename @objname = @full_table_name, @newname = @hidden_name, @objtype = 'OBJECT';

-- rename mock to original
EXECUTE sp_rename @objname = @full_mock_name, @newname = @table_name, @objtype = 'OBJECT';

GO
