CREATE TABLE [dbo].[s_exacttarget_subscribers] (
    [s_exacttarget_subscribers_id] BIGINT         IDENTITY (1, 1) NOT NULL,
    [bk_hash]                      CHAR (32)      NOT NULL,
    [client_id]                    BIGINT         NULL,
    [subscriber_key]               VARCHAR (4000) NULL,
    [email_address]                VARCHAR (4000) NULL,
    [subscriber_id]                BIGINT         NULL,
    [status]                       VARCHAR (4000) NULL,
    [date_held]                    DATETIME       NULL,
    [date_created]                 DATETIME       NULL,
    [date_unsubscribed]            DATETIME       NULL,
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
WITH (CLUSTERED INDEX([bk_hash], [s_exacttarget_subscribers_id]), DISTRIBUTION = HASH([bk_hash]));


GO
CREATE NONCLUSTERED INDEX [ix_dv_batch_id]
    ON [dbo].[s_exacttarget_subscribers]([dv_batch_id] ASC);

