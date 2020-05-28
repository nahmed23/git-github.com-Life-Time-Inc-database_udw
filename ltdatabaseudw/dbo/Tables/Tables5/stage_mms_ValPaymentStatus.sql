CREATE TABLE [dbo].[stage_mms_ValPaymentStatus] (
    [stage_mms_ValPaymentStatus_id] BIGINT       NOT NULL,
    [ValPaymentStatusID]            INT          NULL,
    [Description]                   VARCHAR (50) NULL,
    [SortOrder]                     INT          NULL,
    [InsertedDateTime]              DATETIME     NULL,
    [UpdatedDateTime]               DATETIME     NULL,
    [dv_batch_id]                   BIGINT       NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

