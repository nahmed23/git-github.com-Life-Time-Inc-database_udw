﻿CREATE TABLE [dbo].[stage_hash_mms_PTCreditCardBatch] (
    [stage_hash_mms_PTCreditCardBatch_id] BIGINT          IDENTITY (1, 1) NOT NULL,
    [bk_hash]                             CHAR (32)       NOT NULL,
    [PTCreditCardBatchID]                 INT             NULL,
    [PTCreditCardTerminalID]              INT             NULL,
    [BatchNumber]                         SMALLINT        NULL,
    [TransactionCount]                    INT             NULL,
    [NetAmount]                           DECIMAL (10, 3) NULL,
    [ActionCode]                          VARCHAR (2)     NULL,
    [ResponseCode]                        VARCHAR (6)     NULL,
    [ResponseMessage]                     VARCHAR (50)    NULL,
    [OpenDateTime]                        DATETIME        NULL,
    [UTCOpenDateTime]                     DATETIME        NULL,
    [OpenDateTimeZone]                    VARCHAR (4)     NULL,
    [CloseDateTime]                       DATETIME        NULL,
    [UTCCloseDateTime]                    DATETIME        NULL,
    [CloseDateTimeZone]                   VARCHAR (4)     NULL,
    [SubmitDateTime]                      DATETIME        NULL,
    [UTCSubmitDateTime]                   DATETIME        NULL,
    [SubmitDateTimeZone]                  VARCHAR (4)     NULL,
    [ValCreditCardBatchStatusID]          SMALLINT        NULL,
    [InsertedDateTime]                    DATETIME        NULL,
    [UpdatedDateTime]                     DATETIME        NULL,
    [DrawerActivityID]                    INT             NULL,
    [SubmittedEmployeeID]                 INT             NULL,
    [dv_load_date_time]                   DATETIME        NOT NULL,
    [dv_inserted_date_time]               DATETIME        NOT NULL,
    [dv_insert_user]                      VARCHAR (50)    NOT NULL,
    [dv_updated_date_time]                DATETIME        NULL,
    [dv_update_user]                      VARCHAR (50)    NULL,
    [dv_batch_id]                         BIGINT          NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));

