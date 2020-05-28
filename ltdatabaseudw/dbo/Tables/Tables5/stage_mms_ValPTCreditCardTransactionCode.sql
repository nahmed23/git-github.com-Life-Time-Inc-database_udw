CREATE TABLE [dbo].[stage_mms_ValPTCreditCardTransactionCode] (
    [stage_mms_ValPTCreditCardTransactionCode_id] BIGINT       NOT NULL,
    [ValPTCreditCardTransactionCodeID]            SMALLINT     NULL,
    [Description]                                 VARCHAR (50) NULL,
    [SortOrder]                                   SMALLINT     NULL,
    [TransactionCode]                             SMALLINT     NULL,
    [InsertedDateTime]                            DATETIME     NULL,
    [UpdatedDateTime]                             DATETIME     NULL,
    [dv_batch_id]                                 BIGINT       NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([dv_batch_id]));

