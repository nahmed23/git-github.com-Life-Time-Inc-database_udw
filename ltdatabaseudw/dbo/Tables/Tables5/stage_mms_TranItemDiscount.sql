CREATE TABLE [dbo].[stage_mms_TranItemDiscount] (
    [stage_mms_TranItemDiscount_id] BIGINT          NOT NULL,
    [TranItemDiscountID]            INT             NULL,
    [TranItemID]                    INT             NULL,
    [PricingDiscountID]             INT             NULL,
    [AppliedDiscountAmount]         DECIMAL (26, 6) NULL,
    [InsertedDateTime]              DATETIME        NULL,
    [UpdatedDateTime]               DATETIME        NULL,
    [PromotionCode]                 VARCHAR (50)    NULL,
    [ValDiscountReasonID]           SMALLINT        NULL,
    [dv_batch_id]                   BIGINT          NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

