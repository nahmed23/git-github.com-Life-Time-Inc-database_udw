CREATE TABLE [dbo].[stage_mms_MMSTranRefund] (
    [stage_mms_MMSTranRefund_id] BIGINT   NOT NULL,
    [MMSTranRefundID]            INT      NULL,
    [MMSTranID]                  INT      NULL,
    [RequestingClubID]           INT      NULL,
    [InsertedDateTime]           DATETIME NULL,
    [UpdatedDateTime]            DATETIME NULL,
    [dv_batch_id]                BIGINT   NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

