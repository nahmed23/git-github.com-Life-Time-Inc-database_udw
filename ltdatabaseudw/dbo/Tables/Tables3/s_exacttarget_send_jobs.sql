CREATE TABLE [dbo].[s_exacttarget_send_jobs] (
    [s_exacttarget_send_jobs_id]   BIGINT         IDENTITY (1, 1) NOT NULL,
    [bk_hash]                      CHAR (32)      NOT NULL,
    [client_id]                    BIGINT         NULL,
    [send_id]                      BIGINT         NULL,
    [from_name]                    VARCHAR (4000) NULL,
    [from_email]                   VARCHAR (4000) NULL,
    [sched_time]                   DATETIME       NULL,
    [sent_time]                    DATETIME       NULL,
    [subject]                      VARCHAR (4000) NULL,
    [email_name]                   VARCHAR (4000) NULL,
    [triggered_send_external_key]  VARCHAR (4000) NULL,
    [send_definition_external_key] VARCHAR (4000) NULL,
    [job_status]                   VARCHAR (4000) NULL,
    [preview_url]                  VARCHAR (4000) NULL,
    [is_multipart]                 VARCHAR (4000) NULL,
    [additional]                   VARCHAR (4000) NULL,
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
WITH (CLUSTERED INDEX([bk_hash], [s_exacttarget_send_jobs_id]), DISTRIBUTION = HASH([bk_hash]));


GO
CREATE NONCLUSTERED INDEX [ix_dv_batch_id]
    ON [dbo].[s_exacttarget_send_jobs]([dv_batch_id] ASC);

