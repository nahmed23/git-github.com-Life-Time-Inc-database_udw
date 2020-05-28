CREATE TABLE [dbo].[stage_mms_TranItemRefund] (
    [stage_mms_TranItemRefund_id] BIGINT   NOT NULL,
    [TranItemRefundID]            INT      NULL,
    [TranItemID]                  INT      NULL,
    [OriginalTranItemID]          INT      NULL,
    [InsertedDateTime]            DATETIME NULL,
    [UpdatedDateTime]             DATETIME NULL,
    [dv_batch_id]                 BIGINT   NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

