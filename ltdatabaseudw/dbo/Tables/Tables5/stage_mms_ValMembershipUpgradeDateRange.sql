CREATE TABLE [dbo].[stage_mms_ValMembershipUpgradeDateRange] (
    [stage_mms_ValMembershipUpgradeDateRange_id] BIGINT       NOT NULL,
    [ValMembershipUpgradeDateRangeID]            INT          NULL,
    [Description]                                VARCHAR (50) NULL,
    [SortOrder]                                  INT          NULL,
    [InsertedDateTime]                           DATETIME     NULL,
    [UpdatedDateTime]                            DATETIME     NULL,
    [dv_batch_id]                                BIGINT       NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([dv_batch_id]));

