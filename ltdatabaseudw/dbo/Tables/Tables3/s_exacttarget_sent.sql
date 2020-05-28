CREATE TABLE [dbo].[s_exacttarget_sent] (
    [s_exacttarget_sent_id]       BIGINT         IDENTITY (1, 1) NOT NULL,
    [bk_hash]                     CHAR (32)      NOT NULL,
    [client_id]                   BIGINT         NULL,
    [send_id]                     BIGINT         NULL,
    [subscriber_key]              VARCHAR (4000) NULL,
    [email_address]               VARCHAR (4000) NULL,
    [subscriber_id]               BIGINT         NULL,
    [list_id]                     BIGINT         NULL,
    [event_date]                  DATETIME       NULL,
    [event_type]                  VARCHAR (4000) NULL,
    [batch_id]                    VARCHAR (4000) NULL,
    [triggered_send_external_key] VARCHAR (4000) NULL,
    [campaign_id]                 VARCHAR (4000) NULL,
    [jan_one]                     DATETIME       NULL,
    [dv_load_date_time]           DATETIME       NOT NULL,
    [dv_r_load_source_id]         BIGINT         NOT NULL,
    [dv_inserted_date_time]       DATETIME       NOT NULL,
    [dv_insert_user]              VARCHAR (50)   NOT NULL,
    [dv_updated_date_time]        DATETIME       NULL,
    [dv_update_user]              VARCHAR (50)   NULL,
    [dv_hash]                     CHAR (32)      NOT NULL,
    [dv_batch_id]                 BIGINT         NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));


GO
CREATE CLUSTERED COLUMNSTORE INDEX [cci_s_exacttarget_sent]
    ON [dbo].[s_exacttarget_sent];


GO
CREATE STATISTICS [stat_s_exacttarget_sent]
    ON [dbo].[s_exacttarget_sent]([dv_batch_id]);


GO
CREATE STATISTICS [stat_s_exacttarget_sent_campaign_id]
    ON [dbo].[s_exacttarget_sent]([campaign_id]);

