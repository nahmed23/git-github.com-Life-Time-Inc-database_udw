CREATE TABLE [dbo].[stage_exacttarget_NotSent] (
    [stage_exacttarget_NotSent_id] BIGINT         NOT NULL,
    [ClientID]                     BIGINT         NULL,
    [SendID]                       BIGINT         NULL,
    [SubscriberKey]                VARCHAR (4000) NULL,
    [EmailAddress]                 VARCHAR (4000) NULL,
    [SubscriberID]                 BIGINT         NULL,
    [ListID]                       BIGINT         NULL,
    [EventDate]                    DATETIME       NULL,
    [EventType]                    VARCHAR (4000) NULL,
    [BatchID]                      VARCHAR (4000) NULL,
    [TriggeredSendExternalKey]     VARCHAR (4000) NULL,
    [Reason]                       VARCHAR (4000) NULL,
    [jan_one]                      DATETIME       NULL,
    [dv_inserted_date_time]        DATETIME       NOT NULL,
    [dv_insert_user]               VARCHAR (50)   NOT NULL,
    [dv_updated_date_time]         DATETIME       NULL,
    [dv_update_user]               VARCHAR (50)   NULL,
    [dv_batch_id]                  BIGINT         NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

