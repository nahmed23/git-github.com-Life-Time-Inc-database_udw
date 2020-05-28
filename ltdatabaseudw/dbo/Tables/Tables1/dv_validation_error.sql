CREATE TABLE [dbo].[dv_validation_error] (
    [dv_validation_error_id]   INT            NULL,
    [table_name]               VARCHAR (500)  NULL,
    [validation_error_message] VARCHAR (8000) NULL,
    [error_count]              BIGINT         NULL,
    [dv_inserted_date_time]    DATETIME       NOT NULL,
    [dv_insert_user]           VARCHAR (50)   NOT NULL,
    [dv_updated_date_time]     DATETIME       NULL,
    [dv_update_user]           VARCHAR (50)   NULL,
    [dv_batch_id]              BIGINT         NULL
)
WITH (HEAP, DISTRIBUTION = HASH([dv_batch_id]));

