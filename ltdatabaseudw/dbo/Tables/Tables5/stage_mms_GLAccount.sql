CREATE TABLE [dbo].[stage_mms_GLAccount] (
    [stage_mms_GLAccount_id] BIGINT       NOT NULL,
    [GLAccountID]            INT          NULL,
    [RevenueGLAccountNumber] VARCHAR (10) NULL,
    [RefundGLAccountNumber]  VARCHAR (10) NULL,
    [InsertedDateTime]       DATETIME     NULL,
    [UpdatedDateTime]        DATETIME     NULL,
    [DiscountGLAccount]      VARCHAR (10) NULL,
    [dv_batch_id]            BIGINT       NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

