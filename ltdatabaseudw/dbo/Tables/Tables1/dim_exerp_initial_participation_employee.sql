CREATE TABLE [dbo].[dim_exerp_initial_participation_employee] (
    [fact_mms_sales_transaction_item_key] CHAR (32) NULL,
    [mms_tran_id]                         INT       NULL,
    [tran_item_id]                        INT       NULL,
    [sale_dim_employee_key]               CHAR (32) NULL,
    [sale_employee_id]                    INT       NULL,
    [service_dim_employee_key]            CHAR (32) NULL,
    [service_employee_id]                 INT       NULL
)
WITH (CLUSTERED COLUMNSTORE INDEX, DISTRIBUTION = REPLICATE);

