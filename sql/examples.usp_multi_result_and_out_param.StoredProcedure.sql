SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
create   procedure [examples].[usp_multi_result_and_out_param]
    @customer_id varchar(50)
    ,@order_count int output
as

    select * 
    from examples.Customers 
    where CustomerID =  @customer_id;

    select * 
    from examples.Orders 
    where CustomerID = @customer_id;

    set @order_count = @@ROWCOUNT;

    return @order_count;
GO
