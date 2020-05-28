CREATE TABLE [dbo].[stage_mms_ValCurrencyCode] (
    [stage_mms_ValCurrencyCode_id] BIGINT       NOT NULL,
    [ValCurrencyCodeID]            INT          NULL,
    [Description]                  VARCHAR (50) NULL,
    [CurrencyCode]                 CHAR (3)     NULL,
    [SortOrder]                    INT          NULL,
    [InsertedDateTime]             DATETIME     NULL,
    [UpdatedDateTime]              DATETIME     NULL,
    [dv_batch_id]                  BIGINT       NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([dv_batch_id]));

