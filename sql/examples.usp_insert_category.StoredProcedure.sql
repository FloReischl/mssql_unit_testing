SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

create   procedure [examples].[usp_insert_category]
    @name NVARCHAR(15)
    ,@description NVARCHAR(max) = ''
    ,@picture varbinary(max) = NULL
AS
    set nocount on;

    insert into examples.Categories (CategoryName, [Description], Picture)
        values (@name, @description, @picture);

    return scope_identity();
GO
