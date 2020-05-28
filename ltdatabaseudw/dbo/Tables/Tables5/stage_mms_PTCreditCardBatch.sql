CREATE TABLE [dbo].[stage_mms_PTCreditCardBatch] (
    [stage_mms_PTCreditCardBatch_id] BIGINT          NOT NULL,
    [PTCreditCardBatchID]            INT             NULL,
    [PTCreditCardTerminalID]         INT             NULL,
    [BatchNumber]                    SMALLINT        NULL,
    [TransactionCount]               INT             NULL,
    [NetAmount]                      DECIMAL (10, 3) NULL,
    [ActionCode]                     VARCHAR (2)     NULL,
    [ResponseCode]                   VARCHAR (6)     NULL,
    [ResponseMessage]                VARCHAR (50)    NULL,
    [OpenDateTime]                   DATETIME        NULL,
    [UTCOpenDateTime]                DATETIME        NULL,
    [OpenDateTimeZone]               VARCHAR (4)     NULL,
    [CloseDateTime]                  DATETIME        NULL,
    [UTCCloseDateTime]               DATETIME        NULL,
    [CloseDateTimeZone]              VARCHAR (4)     NULL,
    [SubmitDateTime]                 DATETIME        NULL,
    [UTCSubmitDateTime]              DATETIME        NULL,
    [SubmitDateTimeZone]             VARCHAR (4)     NULL,
    [ValCreditCardBatchStatusID]     SMALLINT        NULL,
    [InsertedDateTime]               DATETIME        NULL,
    [UpdatedDateTime]                DATETIME        NULL,
    [DrawerActivityID]               INT             NULL,
    [SubmittedEmployeeID]            INT             NULL,
    [dv_batch_id]                    BIGINT          NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

