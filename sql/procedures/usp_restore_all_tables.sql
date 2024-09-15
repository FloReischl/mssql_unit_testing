CREATE OR ALTER PROCEDURE mrtest.usp_restore_all_tables
AS

DECLARE @hidden_tables TABLE (
    object_id INT NOT NULL PRIMARY KEY
    ,schema_name NVARCHAR(256) NOT NULL
    ,table_name NVARCHAR(256) NOT NULL
);

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
    WHERE name like '%_hidden_mrtest'
    ;

DECLARE @object_id INT;
DECLARE @schema_name NVARCHAR(256);
DECLARE @table_name NVARCHAR(256);

WHILE EXISTS (SELECT * FROM @hidden_tables)
BEGIN
    SELECT TOP(1)
        @object_id = object_id
        ,@schema_name = schema_name
        ,@table_name = table_name
    FROM @hidden_tables
    ;

    EXECUTE mrtest.usp_mock_table @schema_name = @schema_name, @table_name = @table_name;

    DELETE FROM @hidden_tables WHERE object_id = @object_id;
END;

GO
