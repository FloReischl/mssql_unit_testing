/****** Object:  StoredProcedure [mrtest].[usp_get_all_tables_to_restore]    Script Date: 2024-09-19 16:31:02 ******/
DROP PROCEDURE [mrtest].[usp_get_all_tables_to_restore]
GO
/****** Object:  StoredProcedure [mrtest].[usp_get_all_tables_to_restore]    Script Date: 2024-09-19 16:31:02 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
create   PROCEDURE [mrtest].[usp_get_all_tables_to_restore]
AS

-- collect all hidden tables
SELECT
    object_id
    ,OBJECT_SCHEMA_NAME(object_id)
    ,SUBSTRING(name, 1, LEN(name) - 15)
FROM sys.tables
WHERE name like '%_HIDDEN_MRTEST'
;

GO
