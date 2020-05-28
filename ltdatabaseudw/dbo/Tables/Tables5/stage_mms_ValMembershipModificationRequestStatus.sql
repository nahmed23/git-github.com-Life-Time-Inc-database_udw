CREATE TABLE [dbo].[stage_mms_ValMembershipModificationRequestStatus] (
    [stage_mms_ValMembershipModificationRequestStatus_id] BIGINT       NOT NULL,
    [ValMembershipModificationRequestStatusID]            INT          NULL,
    [Description]                                         VARCHAR (50) NULL,
    [SortOrder]                                           INT          NULL,
    [InsertedDatetime]                                    DATETIME     NULL,
    [UpdatedDateTime]                                     DATETIME     NULL,
    [dv_batch_id]                                         BIGINT       NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

