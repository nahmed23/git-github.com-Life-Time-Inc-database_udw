CREATE TABLE [dbo].[stage_humanity_tasks] (
    [stage_humanity_tasks_id]  BIGINT        NOT NULL,
    [task_id]                  BIGINT        NULL,
    [shift_id]                 BIGINT        NULL,
    [company_id]               VARCHAR (255) NULL,
    [task_name]                VARCHAR (255) NULL,
    [created_at]               VARCHAR (255) NULL,
    [created_by]               VARCHAR (255) NULL,
    [deleted]                  VARCHAR (255) NULL,
    [load_dttm]                VARCHAR (255) NULL,
    [ltf_file_name]            VARCHAR (255) NULL,
    [file_arrive_date]         VARCHAR (255) NULL,
    [dummy_modified_date_time] DATETIME      NULL,
    [dv_batch_id]              BIGINT        NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

