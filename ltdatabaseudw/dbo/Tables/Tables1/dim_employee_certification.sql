CREATE TABLE [dbo].[dim_employee_certification] (
    [dim_employee_certification_id] BIGINT         IDENTITY (1, 1) NOT NULL,
    [certification]                 VARCHAR (1000) NULL,
    [dim_employee_key]              CHAR (32)      NULL,
    [dv_load_date_time]             DATETIME       NULL,
    [dv_load_end_date_time]         DATETIME       NULL,
    [dv_batch_id]                   BIGINT         NOT NULL,
    [dv_inserted_date_time]         DATETIME       NOT NULL,
    [dv_insert_user]                VARCHAR (50)   NOT NULL,
    [dv_updated_date_time]          DATETIME       NULL,
    [dv_update_user]                VARCHAR (50)   NULL
)
WITH (HEAP, DISTRIBUTION = REPLICATE);

