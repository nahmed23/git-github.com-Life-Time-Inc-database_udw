CREATE PROC [dbo].[proc_util_out_of_etl] AS
begin
    exec proc_dim_exerp_initial_participation_employee
    exec proc_fact_allocated_transaction_item -1

end
