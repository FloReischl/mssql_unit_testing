/****** Object:  StoredProcedure [mrtest].[usp_restore_all_tables]    Script Date: 2024-09-19 16:31:02 ******/
DROP PROCEDURE [mrtest].[usp_restore_all_tables]
GO
/****** Object:  StoredProcedure [mrtest].[usp_restore_all_tables]    Script Date: 2024-09-19 16:31:02 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE   PROCEDURE [mrtest].[usp_restore_all_tables]
AS

SET NOCOUNT ON;

DECLARE @hidden_tables TABLE (
    object_id INT NOT NULL PRIMARY KEY
    ,schema_name NVARCHAR(256) NOT NULL
    ,table_name NVARCHAR(256) NOT NULL
);

DECLARE @restored_tables TABLE (
    schema_name NVARCHAR(256) NOT NULL
    ,table_name NVARCHAR(256) NOT NULL
);

-- collect all hidden tables
INSERT INTO @hidden_tables (
        object_id
        ,schema_name
        ,table_name
    )
    SELECT
        object_id
        ,OBJECT_SCHEMA_NAME(object_id)
        ,SUBSTRING(name, 1, LEN(name) - 14)
    FROM sys.tables
    WHERE name like '%_HIDDEN_MRTEST'
    ;

DECLARE @object_id INT;
DECLARE @schema_name NVARCHAR(256);
DECLARE @table_name NVARCHAR(256);

-- restore all hiden tables
WHILE EXISTS (SELECT * FROM @hidden_tables)
BEGIN
    SELECT TOP(1)
        @object_id = object_id
        ,@schema_name = schema_name
        ,@table_name = table_name
    FROM @hidden_tables
    ;

    EXECUTE mrtest.usp_restore_table @schema_name = @schema_name, @table_name = @table_name;

    DELETE FROM @hidden_tables 
    OUTPUT deleted.schema_name, deleted.table_name
    INTO @restored_tables
    WHERE object_id = @object_id;
END;

SELECT * FROM @restored_tables;

GO
