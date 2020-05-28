CREATE TABLE [dbo].[stage_hash_mms_membershipSalesPromotionCode] (
    [stage_hash_mms_membershipSalesPromotionCode_id] BIGINT    IDENTITY (1, 1) NOT NULL,
    [bk_hash]                                        CHAR (32) NOT NULL,
    [MembershipSalesPromotionCodeID]                 INT       NULL,
    [MembershipID]                                   INT       NULL,
    [MemberID]                                       INT       NULL,
    [SalesPromotionCodeID]                           INT       NULL,
    [SalesAdvisorEmployeeID]                         INT       NULL,
    [InsertedDateTime]                               DATETIME  NULL,
    [UpdatedDateTime]                                DATETIME  NULL,
    [dv_load_date_time]                              DATETIME  NOT NULL,
    [dv_batch_id]                                    BIGINT    NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));

