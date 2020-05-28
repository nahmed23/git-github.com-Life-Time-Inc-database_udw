CREATE TABLE [dbo].[stage_mms_ValPTCreditCardType] (
    [stage_mms_ValPTCreditCardType_id] BIGINT       NOT NULL,
    [ValPTCreditCardTypeID]            INT          NULL,
    [Description]                      VARCHAR (50) NULL,
    [SortOrder]                        INT          NULL,
    [CardType]                         VARCHAR (4)  NULL,
    [InsertedDateTime]                 DATETIME     NULL,
    [UpdatedDateTime]                  DATETIME     NULL,
    [dv_batch_id]                      BIGINT       NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([dv_batch_id]));

