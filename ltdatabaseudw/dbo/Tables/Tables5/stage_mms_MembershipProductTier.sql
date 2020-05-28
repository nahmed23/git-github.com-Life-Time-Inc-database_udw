CREATE TABLE [dbo].[stage_mms_MembershipProductTier] (
    [stage_mms_MembershipProductTier_id] BIGINT   NOT NULL,
    [MembershipProductTierID]            INT      NULL,
    [MembershipID]                       INT      NULL,
    [ProductTierID]                      INT      NULL,
    [InsertedDateTime]                   DATETIME NULL,
    [UpdatedDateTime]                    DATETIME NULL,
    [LastUpdatedEmployeeID]              INT      NULL,
    [dv_batch_id]                        BIGINT   NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

