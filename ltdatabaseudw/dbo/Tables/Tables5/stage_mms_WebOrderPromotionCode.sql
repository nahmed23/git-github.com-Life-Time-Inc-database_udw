CREATE TABLE [dbo].[stage_mms_WebOrderPromotionCode] (
    [stage_mms_WebOrderPromotionCode_id] BIGINT       NOT NULL,
    [WebOrderPromotionCodeID]            INT          NULL,
    [WebOrderID]                         INT          NULL,
    [PromotionCode]                      VARCHAR (50) NULL,
    [InsertedDateTime]                   DATETIME     NULL,
    [UpdatedDateTime]                    DATETIME     NULL,
    [dv_batch_id]                        BIGINT       NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

