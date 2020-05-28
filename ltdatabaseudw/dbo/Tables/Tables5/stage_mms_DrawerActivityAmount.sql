CREATE TABLE [dbo].[stage_mms_DrawerActivityAmount] (
    [stage_mms_DrawerActivityAmount_id] BIGINT          NOT NULL,
    [DrawerActivityAmountID]            INT             NULL,
    [DrawerActivityID]                  INT             NULL,
    [TranTotalAmount]                   DECIMAL (26, 6) NULL,
    [ActualTotalAmount]                 DECIMAL (26, 6) NULL,
    [ValPaymentTypeID]                  TINYINT         NULL,
    [InsertedDateTime]                  DATETIME        NULL,
    [UpdatedDateTime]                   DATETIME        NULL,
    [ValCurrencyCodeID]                 TINYINT         NULL,
    [dv_batch_id]                       BIGINT          NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

