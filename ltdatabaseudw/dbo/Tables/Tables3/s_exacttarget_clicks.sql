CREATE TABLE [dbo].[s_exacttarget_clicks] (
    [s_exacttarget_clicks_id]     BIGINT          IDENTITY (1, 1) NOT NULL,
    [bk_hash]                     CHAR (32)       NOT NULL,
    [stage_exacttarget_clicks_id] BIGINT          NULL,
    [client_id]                   BIGINT          NULL,
    [send_id]                     BIGINT          NULL,
    [subscriber_key]              VARCHAR (4000)  NULL,
    [email_address]               VARCHAR (4000)  NULL,
    [subscriber_id]               BIGINT          NULL,
    [list_id]                     BIGINT          NULL,
    [event_date]                  DATETIME        NULL,
    [event_type]                  VARCHAR (4000)  NULL,
    [send_url_id]                 BIGINT          NULL,
    [batch_id]                    VARCHAR (4000)  NULL,
    [url]                         VARCHAR (4000)  NULL,
    [alias]                       VARCHAR (4000)  NULL,
    [url_id]                      BIGINT          NULL,
    [triggered_send_external_key] VARCHAR (4000)  NULL,
    [is_unique]                   VARCHAR (4000)  NULL,
    [is_unique_for_url]           VARCHAR (4000)  NULL,
    [ip_address]                  VARCHAR (4000)  NULL,
    [country]                     VARCHAR (4000)  NULL,
    [region]                      VARCHAR (4000)  NULL,
    [city]                        VARCHAR (4000)  NULL,
    [latitude]                    DECIMAL (26, 6) NULL,
    [longitude]                   DECIMAL (26, 6) NULL,
    [metro_code]                  VARCHAR (4000)  NULL,
    [area_code]                   INT             NULL,
    [browser]                     VARCHAR (4000)  NULL,
    [email_client]                VARCHAR (4000)  NULL,
    [operating_system]            VARCHAR (4000)  NULL,
    [device]                      VARCHAR (4000)  NULL,
    [jan_one]                     DATETIME        NULL,
    [dv_load_date_time]           DATETIME        NOT NULL,
    [dv_batch_id]                 BIGINT          NOT NULL,
    [dv_r_load_source_id]         BIGINT          NOT NULL,
    [dv_inserted_date_time]       DATETIME        NOT NULL,
    [dv_insert_user]              VARCHAR (50)    NOT NULL,
    [dv_updated_date_time]        DATETIME        NULL,
    [dv_update_user]              VARCHAR (50)    NULL,
    [dv_hash]                     CHAR (32)       NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));


GO
CREATE CLUSTERED INDEX [ci_s_exacttarget_clicks]
    ON [dbo].[s_exacttarget_clicks]([bk_hash] ASC, [s_exacttarget_clicks_id] ASC);


GO
CREATE NONCLUSTERED INDEX [ix_dv_batch_id]
    ON [dbo].[s_exacttarget_clicks]([dv_batch_id] ASC);

