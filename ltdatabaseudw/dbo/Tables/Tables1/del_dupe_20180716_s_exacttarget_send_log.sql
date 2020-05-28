CREATE TABLE [dbo].[del_dupe_20180716_s_exacttarget_send_log] (
    [s_exacttarget_send_log_id] BIGINT         NOT NULL,
    [bk_hash]                   CHAR (32)      NOT NULL,
    [job_id]                    BIGINT         NULL,
    [list_id]                   BIGINT         NULL,
    [batch_id]                  BIGINT         NULL,
    [sub_id]                    BIGINT         NULL,
    [triggered_send_id]         BIGINT         NULL,
    [error_code]                VARCHAR (4000) NULL,
    [member_id]                 BIGINT         NULL,
    [subscriber_key]            VARCHAR (4000) NULL,
    [email_address]             VARCHAR (4000) NULL,
    [inserted_date_time]        DATETIME       NULL,
    [jan_one]                   DATETIME       NULL,
    [dv_load_date_time]         DATETIME       NOT NULL,
    [dv_r_load_source_id]       BIGINT         NOT NULL,
    [dv_inserted_date_time]     DATETIME       NOT NULL,
    [dv_insert_user]            VARCHAR (50)   NOT NULL,
    [dv_updated_date_time]      DATETIME       NULL,
    [dv_update_user]            VARCHAR (50)   NULL,
    [dv_hash]                   CHAR (32)      NOT NULL,
    [dv_batch_id]               BIGINT         NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));

