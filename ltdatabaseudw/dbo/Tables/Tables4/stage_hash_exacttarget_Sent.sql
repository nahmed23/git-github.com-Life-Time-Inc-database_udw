CREATE TABLE [dbo].[stage_hash_exacttarget_Sent] (
    [stage_hash_exacttarget_Sent_id] BIGINT         IDENTITY (1, 1) NOT NULL,
    [bk_hash]                        CHAR (32)      NOT NULL,
    [ClientID]                       BIGINT         NULL,
    [SendID]                         BIGINT         NULL,
    [SubscriberKey]                  VARCHAR (4000) NULL,
    [EmailAddress]                   VARCHAR (4000) NULL,
    [SubscriberID]                   BIGINT         NULL,
    [ListID]                         BIGINT         NULL,
    [EventDate]                      DATETIME       NULL,
    [EventType]                      VARCHAR (4000) NULL,
    [BatchID]                        VARCHAR (4000) NULL,
    [TriggeredSendExternalKey]       VARCHAR (4000) NULL,
    [CampaignID]                     VARCHAR (4000) NULL,
    [jan_one]                        DATETIME       NULL,
    [dv_load_date_time]              DATETIME       NOT NULL,
    [dv_inserted_date_time]          DATETIME       NOT NULL,
    [dv_insert_user]                 VARCHAR (50)   NOT NULL,
    [dv_updated_date_time]           DATETIME       NULL,
    [dv_update_user]                 VARCHAR (50)   NULL,
    [dv_batch_id]                    BIGINT         NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));

