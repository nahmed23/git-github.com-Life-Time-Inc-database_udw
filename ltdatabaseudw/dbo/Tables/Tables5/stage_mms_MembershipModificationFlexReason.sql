CREATE TABLE [dbo].[stage_mms_MembershipModificationFlexReason] (
    [stage_mms_MembershipModificationFlexReason_id] BIGINT   NOT NULL,
    [MembershipModificationFlexReasonID]            INT      NULL,
    [MembershipModificationRequestID]               INT      NULL,
    [ValFlexReasonID]                               INT      NULL,
    [InsertedDateTime]                              DATETIME NULL,
    [UpdatedDateTime]                               DATETIME NULL,
    [dv_batch_id]                                   BIGINT   NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

