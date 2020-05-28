CREATE TABLE [dbo].[fact_humanity_tasks] (
    [fact_humanity_tasks_id]  BIGINT        IDENTITY (1, 1) NOT NULL,
    [company_id]              VARCHAR (255) NULL,
    [created_at]              VARCHAR (255) NULL,
    [created_by]              VARCHAR (255) NULL,
    [deleted]                 VARCHAR (255) NULL,
    [deleted_flag]            INT           NULL,
    [fact_humanity_tasks_key] CHAR (32)     NULL,
    [file_arrive_date]        DATE          NULL,
    [load_dttm]               VARCHAR (255) NULL,
    [shift_id]                BIGINT        NULL,
    [task_id]                 BIGINT        NULL,
    [task_name]               VARCHAR (255) NULL,
    [dv_load_date_time]       DATETIME      NULL,
    [dv_load_end_date_time]   DATETIME      NULL,
    [dv_batch_id]             BIGINT        NOT NULL,
    [dv_inserted_date_time]   DATETIME      NOT NULL,
    [dv_insert_user]          VARCHAR (50)  NOT NULL,
    [dv_updated_date_time]    DATETIME      NULL,
    [dv_update_user]          VARCHAR (50)  NULL
)
WITH (CLUSTERED COLUMNSTORE INDEX, DISTRIBUTION = HASH([fact_humanity_tasks_key]));

