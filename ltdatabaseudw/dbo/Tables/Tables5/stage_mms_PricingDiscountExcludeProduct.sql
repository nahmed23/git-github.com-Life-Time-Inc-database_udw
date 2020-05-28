CREATE TABLE [dbo].[stage_mms_PricingDiscountExcludeProduct] (
    [stage_mms_PricingDiscountExcludeProduct_id] BIGINT   NOT NULL,
    [PricingDiscountExcludeProductID]            INT      NULL,
    [PricingDiscountID]                          INT      NULL,
    [ExcludeProductID]                           INT      NULL,
    [InsertedDateTime]                           DATETIME NULL,
    [UpdatedDateTime]                            DATETIME NULL,
    [dv_batch_id]                                BIGINT   NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

