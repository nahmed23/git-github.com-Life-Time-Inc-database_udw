CREATE TABLE [dbo].[stage_mms_TranItemGiftCardIssuance] (
    [stage_mms_TranItemGiftCardIssuance_id] BIGINT          NOT NULL,
    [TranItemGiftCardIssuanceID]            INT             NULL,
    [TranItemID]                            INT             NULL,
    [IssuanceAmount]                        DECIMAL (26, 6) NULL,
    [PTStoredValueCardTransactionID]        INT             NULL,
    [InsertedDateTime]                      DATETIME        NULL,
    [UpdatedDateTime]                       DATETIME        NULL,
    [dv_batch_id]                           BIGINT          NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

