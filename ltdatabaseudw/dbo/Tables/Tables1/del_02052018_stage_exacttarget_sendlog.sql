CREATE TABLE [dbo].[del_02052018_stage_exacttarget_sendlog] (
    [stage_exacttarget_SendLog_id] BIGINT         NOT NULL,
    [JobID]                        BIGINT         NULL,
    [ListID]                       BIGINT         NULL,
    [BatchID]                      BIGINT         NULL,
    [SubID]                        BIGINT         NULL,
    [TriggeredSendID]              BIGINT         NULL,
    [ErrorCode_]                   VARCHAR (4000) NULL,
    [Member_ID]                    BIGINT         NULL,
    [SubscriberKey]                VARCHAR (4000) NULL,
    [EmailAddress]                 VARCHAR (4000) NULL,
    [InsertedDateTime]             DATETIME       NULL,
    [jan_one]                      DATETIME       NULL,
    [dv_inserted_date_time]        DATETIME       NOT NULL,
    [dv_insert_user]               VARCHAR (50)   NOT NULL,
    [dv_updated_date_time]         DATETIME       NULL,
    [dv_update_user]               VARCHAR (50)   NULL,
    [dv_batch_id]                  BIGINT         NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

