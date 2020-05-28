CREATE TABLE [dbo].[stage_mms_ValCreditCardBatchStatus] (
    [stage_mms_ValCreditCardBatchStatus_id] BIGINT       NOT NULL,
    [ValCreditCardBatchStatusID]            SMALLINT     NULL,
    [Description]                           VARCHAR (50) NULL,
    [SortOrder]                             SMALLINT     NULL,
    [InsertedDateTime]                      DATETIME     NULL,
    [UpdatedDateTime]                       DATETIME     NULL,
    [dv_batch_id]                           BIGINT       NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([dv_batch_id]));

