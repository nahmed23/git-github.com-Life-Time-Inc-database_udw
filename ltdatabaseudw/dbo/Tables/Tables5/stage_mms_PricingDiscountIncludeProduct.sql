CREATE TABLE [dbo].[stage_mms_PricingDiscountIncludeProduct] (
    [stage_mms_PricingDiscountIncludeProduct_id] BIGINT          NOT NULL,
    [PricingDiscountIncludeProductID]            INT             NULL,
    [PricingDiscountID]                          INT             NULL,
    [ProductID]                                  INT             NULL,
    [TriggerQuantity]                            INT             NULL,
    [DiscountedProductID]                        INT             NULL,
    [DiscountUseLimit]                           INT             NULL,
    [InsertedDateTime]                           DATETIME        NULL,
    [UpdatedDateTime]                            DATETIME        NULL,
    [OverrideDiscountTypeID]                     TINYINT         NULL,
    [OverrideDiscountValue]                      DECIMAL (10, 2) NULL,
    [OverrideSalesCommissionPercent]             DECIMAL (6, 2)  NULL,
    [OverrideServiceCommissionPercent]           DECIMAL (6, 2)  NULL,
    [BundleProductFlag]                          BIT             NULL,
    [dv_batch_id]                                BIGINT          NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

