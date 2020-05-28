CREATE TABLE [dbo].[d_mms_package_adjustment] (
    [d_mms_package_adjustment_id]         BIGINT          IDENTITY (1, 1) NOT NULL,
    [bk_hash]                             CHAR (32)       NOT NULL,
    [fact_mms_package_adjustment_key]     CHAR (32)       NULL,
    [package_adjustment_id]               INT             NULL,
    [adjusted_date_time]                  DATETIME        NULL,
    [adjusted_dim_date_key]               CHAR (8)        NULL,
    [adjusted_dim_time_key]               CHAR (8)        NULL,
    [adjustment_comment]                  VARCHAR (255)   NULL,
    [adjustment_dim_employee_key]         CHAR (32)       NULL,
    [adjustment_mms_tran_id]              INT             NULL,
    [adjustment_type_dim_description_key] CHAR (255)      NULL,
    [fact_mms_package_key]                CHAR (32)       NULL,
    [number_of_sessions_adjusted]         SMALLINT        NULL,
    [package_adjustment_amount]           DECIMAL (26, 6) NULL,
    [p_mms_package_adjustment_id]         BIGINT          NOT NULL,
    [deleted_flag]                        INT             NULL,
    [dv_load_date_time]                   DATETIME        NULL,
    [dv_load_end_date_time]               DATETIME        NULL,
    [dv_batch_id]                         BIGINT          NOT NULL,
    [dv_inserted_date_time]               DATETIME        NOT NULL,
    [dv_insert_user]                      VARCHAR (50)    NOT NULL,
    [dv_updated_date_time]                DATETIME        NULL,
    [dv_update_user]                      VARCHAR (50)    NULL
)
WITH (CLUSTERED COLUMNSTORE INDEX, DISTRIBUTION = HASH([bk_hash]));

