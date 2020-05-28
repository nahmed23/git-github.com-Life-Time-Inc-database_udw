CREATE TABLE [dbo].[s_ec_plans] (
    [s_ec_plans_id]         BIGINT          IDENTITY (1, 1) NOT NULL,
    [bk_hash]               CHAR (32)       NOT NULL,
    [plan_id]               INT             NULL,
    [name]                  NVARCHAR (4000) NULL,
    [duration]              NVARCHAR (50)   NULL,
    [duration_type]         INT             NULL,
    [start_date]            DATETIME        NULL,
    [end_date]              DATETIME        NULL,
    [source_type]           INT             NULL,
    [created_date]          DATETIME        NULL,
    [updated_date]          DATETIME        NULL,
    [dv_load_date_time]     DATETIME        NOT NULL,
    [dv_r_load_source_id]   BIGINT          NOT NULL,
    [dv_inserted_date_time] DATETIME        NOT NULL,
    [dv_insert_user]        VARCHAR (50)    NOT NULL,
    [dv_updated_date_time]  DATETIME        NULL,
    [dv_update_user]        VARCHAR (50)    NULL,
    [dv_hash]               CHAR (32)       NOT NULL,
    [dv_batch_id]           BIGINT          NOT NULL
)
WITH (CLUSTERED INDEX([bk_hash]), DISTRIBUTION = HASH([bk_hash]));


GO
CREATE NONCLUSTERED INDEX [ix_dv_batch_id]
    ON [dbo].[s_ec_plans]([dv_batch_id] ASC);

