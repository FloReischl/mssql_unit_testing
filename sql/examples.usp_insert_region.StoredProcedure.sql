SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
create   procedure [examples].[usp_insert_region] 
    @id INT
    ,@description NCHAR(50)
AS
    set nocount on;

    insert into examples.Region 
        select @id, @description
        ;
GO
