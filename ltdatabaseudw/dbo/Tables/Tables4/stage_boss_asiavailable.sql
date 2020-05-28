CREATE TABLE [dbo].[stage_boss_asiavailable] (
    [stage_boss_asiavailable_id] BIGINT   NOT NULL,
    [club]                       INT      NULL,
    [resource_id]                INT      NULL,
    [start_time]                 DATETIME NULL,
    [end_time]                   DATETIME NULL,
    [schedule_type]              CHAR (1) NULL,
    [dv_batch_id]                BIGINT   NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

