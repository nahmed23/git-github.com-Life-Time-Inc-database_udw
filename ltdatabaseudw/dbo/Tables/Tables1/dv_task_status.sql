CREATE TABLE [dbo].[dv_task_status] (
    [dv_task_status_id] BIGINT        NOT NULL,
    [task]              VARCHAR (500) NOT NULL,
    [task_description]  VARCHAR (500) NOT NULL,
    [task_date_time]    DATETIME      NOT NULL,
    [dv_batch_id]       BIGINT        NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

