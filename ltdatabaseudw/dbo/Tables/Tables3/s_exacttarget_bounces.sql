CREATE TABLE [dbo].[s_exacttarget_bounces] (
    [s_exacttarget_bounces_id]     BIGINT         IDENTITY (1, 1) NOT NULL,
    [bk_hash]                      CHAR (32)      NOT NULL,
    [stage_exacttarget_bounces_id] BIGINT         NULL,
    [client_id]                    BIGINT         NULL,
    [send_id]                      BIGINT         NULL,
    [subscriber_key]               VARCHAR (4000) NULL,
    [email_address]                VARCHAR (4000) NULL,
    [subscriber_id]                BIGINT         NULL,
    [list_id]                      BIGINT         NULL,
    [event_date]                   DATETIME       NULL,
    [event_type]                   VARCHAR (4000) NULL,
    [bounce_category]              VARCHAR (4000) NULL,
    [smtp_code]                    BIGINT         NULL,
    [bounce_reason]                VARCHAR (4000) NULL,
    [batch_id]                     VARCHAR (4000) NULL,
    [triggered_send_external_key]  VARCHAR (4000) NULL,
    [jan_one]                      DATETIME       NULL,
    [dv_load_date_time]            DATETIME       NOT NULL,
    [dv_batch_id]                  BIGINT         NOT NULL,
    [dv_r_load_source_id]          BIGINT         NOT NULL,
    [dv_inserted_date_time]        DATETIME       NOT NULL,
    [dv_insert_user]               VARCHAR (50)   NOT NULL,
    [dv_updated_date_time]         DATETIME       NULL,
    [dv_update_user]               VARCHAR (50)   NULL,
    [dv_hash]                      CHAR (32)      NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));


GO
CREATE CLUSTERED INDEX [ci_s_exacttarget_bounces]
    ON [dbo].[s_exacttarget_bounces]([bk_hash] ASC, [s_exacttarget_bounces_id] ASC);


GO
CREATE NONCLUSTERED INDEX [ix_dv_batch_id]
    ON [dbo].[s_exacttarget_bounces]([dv_batch_id] ASC);

