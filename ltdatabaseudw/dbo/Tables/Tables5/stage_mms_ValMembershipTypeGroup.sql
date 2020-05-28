CREATE TABLE [dbo].[stage_mms_ValMembershipTypeGroup] (
    [stage_mms_ValMembershipTypeGroup_id] BIGINT       NOT NULL,
    [ValMembershipTypeGroupID]            INT          NULL,
    [Description]                         VARCHAR (50) NULL,
    [SortOrder]                           INT          NULL,
    [InsertedDateTime]                    DATETIME     NULL,
    [UpdatedDateTime]                     DATETIME     NULL,
    [ValCardLevelID]                      INT          NULL,
    [dv_batch_id]                         BIGINT       NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

