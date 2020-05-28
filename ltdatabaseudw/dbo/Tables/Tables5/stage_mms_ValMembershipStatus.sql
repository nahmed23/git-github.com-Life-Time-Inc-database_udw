CREATE TABLE [dbo].[stage_mms_ValMembershipStatus] (
    [stage_mms_ValMembershipStatus_id] BIGINT       NOT NULL,
    [ValMembershipStatusID]            INT          NULL,
    [Description]                      VARCHAR (50) NULL,
    [SortOrder]                        INT          NULL,
    [ValMembershipMessageTypeID]       INT          NULL,
    [InsertedDateTime]                 DATETIME     NULL,
    [UpdatedDateTime]                  DATETIME     NULL,
    [dv_batch_id]                      BIGINT       NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([dv_batch_id]));

