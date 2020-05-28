CREATE TABLE [dbo].[dim_employee_job_title_history] (
    [dim_employee_job_title_history_id] BIGINT          IDENTITY (1, 1) NOT NULL,
    [effective_date_time]               DATETIME        NULL,
    [expiration_date_time]              DATETIME        NULL,
    [business_titles]                   NVARCHAR (4000) NULL,
    [cf_employment_status]              VARCHAR (250)   NULL,
    [dim_employee_key]                  VARCHAR (32)    NULL,
    [employee_id]                       INT             NULL,
    [is_primary_flag]                   CHAR (1)        NULL,
    [job_codes]                         VARCHAR (250)   NULL,
    [job_profiles]                      NVARCHAR (4000) NULL,
    [marketing_titles]                  NVARCHAR (4000) NULL,
    [dv_load_date_time]                 DATETIME        NULL,
    [dv_load_end_date_time]             DATETIME        NULL,
    [dv_batch_id]                       BIGINT          NOT NULL,
    [dv_inserted_date_time]             DATETIME        NOT NULL,
    [dv_insert_user]                    VARCHAR (50)    NOT NULL,
    [dv_updated_date_time]              DATETIME        NULL,
    [dv_update_user]                    VARCHAR (50)    NULL
)
WITH (CLUSTERED COLUMNSTORE INDEX, DISTRIBUTION = REPLICATE);

