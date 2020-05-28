CREATE TABLE [dbo].[stage_mms_ValReimbursementTerminationReason] (
    [stage_mms_ValReimbursementTerminationReason_id] BIGINT       NOT NULL,
    [ValReimbursementTerminationReasonID]            INT          NULL,
    [Description]                                    VARCHAR (50) NULL,
    [SortOrder]                                      INT          NULL,
    [InsertedDateTime]                               DATETIME     NULL,
    [UpdatedDateTime]                                DATETIME     NULL,
    [dv_batch_id]                                    BIGINT       NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

