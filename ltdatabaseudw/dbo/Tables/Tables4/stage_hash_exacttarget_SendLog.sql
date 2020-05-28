CREATE TABLE [dbo].[stage_hash_exacttarget_SendLog] (
    [stage_hash_exacttarget_SendLog_id] BIGINT         IDENTITY (1, 1) NOT NULL,
    [bk_hash]                           CHAR (32)      NOT NULL,
    [JobID]                             BIGINT         NULL,
    [ListID]                            BIGINT         NULL,
    [BatchID]                           BIGINT         NULL,
    [SubID]                             BIGINT         NULL,
    [TriggeredSendID]                   BIGINT         NULL,
    [ErrorCode_]                        VARCHAR (4000) NULL,
    [Member_ID]                         BIGINT         NULL,
    [SubscriberKey]                     VARCHAR (4000) NULL,
    [EmailAddress]                      VARCHAR (4000) NULL,
    [InsertedDateTime]                  DATETIME       NULL,
    [eid]                               VARCHAR (4000) NULL,
    [contact]                           VARCHAR (4000) NULL,
    [primarylead]                       VARCHAR (4000) NULL,
    [jan_one]                           DATETIME       NULL,
    [dv_load_date_time]                 DATETIME       NOT NULL,
    [dv_batch_id]                       BIGINT         NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

