CREATE TABLE [dbo].[fact_etips_opt_in_employees] (
    [fact_etips_opt_in_employees_id]        BIGINT           IDENTITY (1, 1) NOT NULL,
    [employee_id]                           VARCHAR (255)    NULL,
    [employee_id_pay_start_date_status_key] VARBINARY (8000) NULL,
    [fact_etips_opt_in_employees_key]       CHAR (32)        NULL,
    [file_arrive_date]                      DATE             NULL,
    [pay_card_end_date]                     VARCHAR (255)    NULL,
    [pay_card_start_date]                   VARCHAR (255)    NULL,
    [pay_card_status]                       VARCHAR (255)    NULL,
    [dv_load_date_time]                     DATETIME         NULL,
    [dv_load_end_date_time]                 DATETIME         NULL,
    [dv_batch_id]                           BIGINT           NOT NULL,
    [dv_inserted_date_time]                 DATETIME         NOT NULL,
    [dv_insert_user]                        VARCHAR (50)     NOT NULL,
    [dv_updated_date_time]                  DATETIME         NULL,
    [dv_update_user]                        VARCHAR (50)     NULL
)
WITH (CLUSTERED COLUMNSTORE INDEX, DISTRIBUTION = HASH([fact_etips_opt_in_employees_key]));

