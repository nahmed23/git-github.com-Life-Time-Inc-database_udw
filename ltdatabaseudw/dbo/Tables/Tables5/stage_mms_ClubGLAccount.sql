CREATE TABLE [dbo].[stage_mms_ClubGLAccount] (
    [stage_mms_ClubGLAccount_id]      BIGINT       NOT NULL,
    [ClubGLAccountID]                 INT          NULL,
    [ClubID]                          INT          NULL,
    [ValCurrencyCodeID]               TINYINT      NULL,
    [GLCashEntryCompanyName]          VARCHAR (15) NULL,
    [GLCashEntryAccount]              VARCHAR (4)  NULL,
    [GLReceivablesEntryAccount]       VARCHAR (4)  NULL,
    [GLCashEntryCashSubAccount]       VARCHAR (11) NULL,
    [GLCashEntryCreditCardSubAccount] VARCHAR (11) NULL,
    [GLReceivablesEntrySubAccount]    VARCHAR (11) NULL,
    [GLReceivablesEntryCompanyName]   VARCHAR (15) NULL,
    [InsertedDateTime]                DATETIME     NULL,
    [UpdatedDateTime]                 DATETIME     NULL,
    [dv_batch_id]                     BIGINT       NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

