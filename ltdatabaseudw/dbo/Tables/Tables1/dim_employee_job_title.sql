CREATE TABLE [dbo].[dim_employee_job_title] (
    [dim_employee_job_title_id] BIGINT         IDENTITY (1, 1) NOT NULL,
    [business_title]            VARCHAR (8000) NULL,
    [dim_employee_key]          CHAR (32)      NULL,
    [family]                    VARCHAR (8000) NULL,
    [is_primary_flag]           CHAR (1)       NULL,
    [job_code]                  VARCHAR (8000) NULL,
    [level]                     VARCHAR (8000) NULL,
    [marketing_title]           VARCHAR (8000) NULL,
    [profile]                   VARCHAR (8000) NULL,
    [sub_family]                VARCHAR (8000) NULL,
    [workday_region_id]         VARCHAR (8000) NULL,
    [dv_load_date_time]         DATETIME       NULL,
    [dv_load_end_date_time]     DATETIME       NULL,
    [dv_batch_id]               BIGINT         NOT NULL,
    [dv_inserted_date_time]     DATETIME       NOT NULL,
    [dv_insert_user]            VARCHAR (50)   NOT NULL,
    [dv_updated_date_time]      DATETIME       NULL,
    [dv_update_user]            VARCHAR (50)   NULL
)
WITH (CLUSTERED COLUMNSTORE INDEX, DISTRIBUTION = REPLICATE);


GO
CREATE NONCLUSTERED INDEX [ix_dim_employee_key]
    ON [dbo].[dim_employee_job_title]([dim_employee_key] ASC);

