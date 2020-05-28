CREATE TABLE [dbo].[stage_mms_ReplicationTest] (
    [stage_mms_ReplicationTest_id] BIGINT   NOT NULL,
    [ReplicationTestID]            INT      NULL,
    [ReplicationDateTime]          DATETIME NULL,
    [dv_batch_id]                  BIGINT   NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

