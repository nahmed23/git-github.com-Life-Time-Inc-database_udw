CREATE TABLE [dbo].[stage_mms_MMSTranRefundMMSTran] (
    [stage_mms_MMSTranRefundMMSTran_id] BIGINT   NOT NULL,
    [MMSTranRefundMMSTranID]            INT      NULL,
    [OriginalMMSTranID]                 INT      NULL,
    [MMSTranRefundID]                   INT      NULL,
    [InsertedDateTime]                  DATETIME NULL,
    [UpdatedDateTime]                   DATETIME NULL,
    [dv_batch_id]                       BIGINT   NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

