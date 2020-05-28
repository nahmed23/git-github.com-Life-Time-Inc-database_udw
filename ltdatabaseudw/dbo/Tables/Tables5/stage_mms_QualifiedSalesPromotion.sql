CREATE TABLE [dbo].[stage_mms_QualifiedSalesPromotion] (
    [stage_mms_QualifiedSalesPromotion_id] BIGINT       NOT NULL,
    [QualifiedSalesPromotionID]            INT          NULL,
    [ValQualifiedSalesPromotionTypeID]     TINYINT      NULL,
    [SalesPromotionID]                     INT          NULL,
    [InsertedDateTime]                     DATETIME     NULL,
    [UpdatedDateTime]                      DATETIME     NULL,
    [PromotionName]                        VARCHAR (50) NULL,
    [Description]                          VARCHAR (50) NULL,
    [dv_batch_id]                          BIGINT       NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

