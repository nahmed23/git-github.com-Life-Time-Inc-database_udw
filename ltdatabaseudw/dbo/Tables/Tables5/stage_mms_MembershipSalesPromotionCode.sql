CREATE TABLE [dbo].[stage_mms_MembershipSalesPromotionCode] (
    [stage_mms_MembershipSalesPromotionCode_id] BIGINT   NOT NULL,
    [MembershipSalesPromotionCodeID]            INT      NULL,
    [MembershipID]                              INT      NULL,
    [MemberID]                                  INT      NULL,
    [SalesPromotionCodeID]                      INT      NULL,
    [SalesAdvisorEmployeeID]                    INT      NULL,
    [InsertedDateTime]                          DATETIME NULL,
    [UpdatedDateTime]                           DATETIME NULL,
    [dv_batch_id]                               BIGINT   NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

