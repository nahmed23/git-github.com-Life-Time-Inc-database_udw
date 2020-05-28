CREATE TABLE [dbo].[pre_stage_exacttarget_send_jobs] (
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
    [executionid]                  BIGINT         NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

