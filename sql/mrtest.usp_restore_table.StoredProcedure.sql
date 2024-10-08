/****** Object:  StoredProcedure [mrtest].[usp_restore_table]    Script Date: 2024-09-19 16:31:02 ******/
DROP PROCEDURE [mrtest].[usp_restore_table]
GO
/****** Object:  StoredProcedure [mrtest].[usp_restore_table]    Script Date: 2024-09-19 16:31:02 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE   PROCEDURE [mrtest].[usp_restore_table]
    @schema_name NVARCHAR(256)
    ,@table_name NVARCHAR(256)
AS
SET NOCOUNT ON;

DECLARE @hidden_name NVARCHAR(256) = @table_name + '_HIDDEN_MRTEST';

DECLARE @full_table_name NVARCHAR(512)  = QUOTENAME(@schema_name) + '.' + QUOTENAME(@table_name);
DECLARE @full_hidden_name NVARCHAR(512) = QUOTENAME(@schema_name) + '.' + QUOTENAME(@hidden_name);

-- check recovery table exists
IF NOT EXISTS (SELECT * FROM sys.tables WHERE object_id = OBJECT_ID(@full_hidden_name))
    THROW 50000, 'Cannot find hidden recovery table. Please verify and fix the database schema.', 1;

-- check current mock table exists
IF EXISTS (SELECT * FROM sys.tables WHERE object_id = OBJECT_ID(@full_table_name))
BEGIN
    --PRINT ('Deleting mock table: ' + @full_table_name);

    -- drop mock table
    EXECUTE ('DROP TABLE ' + @full_table_name);
END;

-- restore original table
EXECUTE sp_rename @objname = @full_hidden_name, @newname = @table_name, @objtype = 'OBJECT';
GO
