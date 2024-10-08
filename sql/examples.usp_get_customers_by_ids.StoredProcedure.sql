SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

create   procedure [examples].[usp_get_customers_by_ids] 
    @ids examples.tt_id_varchar50 READONLY
AS
    select *
    from examples.Customers c
    where exists (
        select * from @ids x where c.CustomerID = cast(x.id AS nchar(5))
    );
GO
