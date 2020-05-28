CREATE VIEW [marketing].[v_fact_exerp_sale_employee_log]
AS select fact_exerp_sale_employee_log.center_id center_id,
       fact_exerp_sale_employee_log.change_dim_employee_key change_dim_employee_key,
       fact_exerp_sale_employee_log.change_person_id change_person_id,
       fact_exerp_sale_employee_log.dim_club_key dim_club_key,
       fact_exerp_sale_employee_log.fact_exerp_sale_employee_log_key fact_exerp_sale_employee_log_key,
       fact_exerp_sale_employee_log.from_dim_date_key from_dim_date_key,
       fact_exerp_sale_employee_log.from_dim_time_key from_dim_time_key,
       fact_exerp_sale_employee_log.sale_dim_employee_key sale_dim_employee_key,
       fact_exerp_sale_employee_log.sale_employee_log_id sale_employee_log_id,
       fact_exerp_sale_employee_log.sale_fact_exerp_transaction_log_key sale_fact_exerp_transaction_log_key,
       fact_exerp_sale_employee_log.sale_id sale_id,
       fact_exerp_sale_employee_log.sale_person_id sale_person_id
  from dbo.fact_exerp_sale_employee_log;