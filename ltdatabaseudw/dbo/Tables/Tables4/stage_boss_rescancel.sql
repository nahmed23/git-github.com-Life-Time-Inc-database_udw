CREATE TABLE [dbo].[stage_boss_rescancel] (
    [stage_boss_rescancel_id] BIGINT   NOT NULL,
    [reservation]             INT      NULL,
    [cancel_date]             DATETIME NULL,
    [dv_batch_id]             BIGINT   NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

