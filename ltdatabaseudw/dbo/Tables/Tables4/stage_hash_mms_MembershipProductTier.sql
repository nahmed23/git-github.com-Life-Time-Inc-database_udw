CREATE TABLE [dbo].[stage_hash_mms_MembershipProductTier] (
    [stage_hash_mms_MembershipProductTier_id] BIGINT       IDENTITY (1, 1) NOT NULL,
    [bk_hash]                                 CHAR (32)    NOT NULL,
    [MembershipProductTierID]                 INT          NULL,
    [MembershipID]                            INT          NULL,
    [ProductTierID]                           INT          NULL,
    [InsertedDateTime]                        DATETIME     NULL,
    [UpdatedDateTime]                         DATETIME     NULL,
    [LastUpdatedEmployeeID]                   INT          NULL,
    [dv_load_date_time]                       DATETIME     NOT NULL,
    [dv_updated_date_time]                    DATETIME     NULL,
    [dv_update_user]                          VARCHAR (50) NULL,
    [dv_batch_id]                             BIGINT       NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));

