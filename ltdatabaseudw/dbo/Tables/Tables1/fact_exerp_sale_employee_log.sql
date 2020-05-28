CREATE TABLE [dbo].[fact_exerp_sale_employee_log] (
    [fact_exerp_sale_employee_log_id]     BIGINT         IDENTITY (1, 1) NOT NULL,
    [center_id]                           INT            NULL,
    [change_dim_employee_key]             VARCHAR (32)   NULL,
    [change_person_id]                    VARCHAR (4000) NULL,
    [dim_club_key]                        VARCHAR (32)   NULL,
    [fact_exerp_sale_employee_log_key]    VARCHAR (32)   NULL,
    [from_dim_date_key]                   VARCHAR (8)    NULL,
    [from_dim_time_key]                   INT            NULL,
    [sale_dim_employee_key]               VARCHAR (32)   NULL,
    [sale_employee_log_id]                VARCHAR (4000) NULL,
    [sale_fact_exerp_transaction_log_key] VARCHAR (32)   NULL,
    [sale_id]                             VARCHAR (4000) NULL,
    [sale_person_id]                      VARCHAR (4000) NULL,
    [dv_load_date_time]                   DATETIME       NULL,
    [dv_load_end_date_time]               DATETIME       NULL,
    [dv_batch_id]                         BIGINT         NOT NULL,
    [dv_inserted_date_time]               DATETIME       NOT NULL,
    [dv_insert_user]                      VARCHAR (50)   NOT NULL,
    [dv_updated_date_time]                DATETIME       NULL,
    [dv_update_user]                      VARCHAR (50)   NULL
)
WITH (CLUSTERED COLUMNSTORE INDEX, DISTRIBUTION = HASH([fact_exerp_sale_employee_log_key]));

