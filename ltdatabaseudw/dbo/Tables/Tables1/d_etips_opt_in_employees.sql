CREATE TABLE [dbo].[d_etips_opt_in_employees] (
    [d_etips_opt_in_employees_id]           BIGINT           IDENTITY (1, 1) NOT NULL,
    [bk_hash]                               CHAR (32)        NOT NULL,
    [d_etips_opt_in_employees_key]          CHAR (32)        NULL,
    [employee_id]                           VARCHAR (255)    NULL,
    [pay_card_start_date]                   VARCHAR (255)    NULL,
    [pay_card_status]                       VARCHAR (255)    NULL,
    [ltf_file_name]                         VARCHAR (255)    NULL,
    [employee_id_pay_start_date_status_key] VARBINARY (8000) NULL,
    [file_arrive_date]                      DATE             NULL,
    [pay_card_end_date]                     VARCHAR (255)    NULL,
    [p_etips_opt_in_employees_id]           BIGINT           NOT NULL,
    [deleted_flag]                          INT              NULL,
    [dv_load_date_time]                     DATETIME         NULL,
    [dv_load_end_date_time]                 DATETIME         NULL,
    [dv_batch_id]                           BIGINT           NOT NULL,
    [dv_inserted_date_time]                 DATETIME         NOT NULL,
    [dv_insert_user]                        VARCHAR (50)     NOT NULL,
    [dv_updated_date_time]                  DATETIME         NULL,
    [dv_update_user]                        VARCHAR (50)     NULL
)
WITH (CLUSTERED COLUMNSTORE INDEX, DISTRIBUTION = HASH([bk_hash]));

