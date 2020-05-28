CREATE PROC [dbo].[proc_fact_ltfmart_employee_goals] AS
begin
    exec proc_dim_exerp_initial_participation_employee
    exec proc_fact_allocated_transaction_item -1

end
