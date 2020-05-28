CREATE TABLE [dbo].[stage_hash_et_sendjobs_bkp_reload] (
    [stage_hash_exacttarget_SendJobs_id] BIGINT         IDENTITY (1, 1) NOT NULL,
    [bk_hash]                            CHAR (32)      NOT NULL,
    [ClientID]                           BIGINT         NULL,
    [SendID]                             BIGINT         NULL,
    [FromName]                           VARCHAR (4000) NULL,
    [FromEmail]                          VARCHAR (4000) NULL,
    [SchedTime]                          DATETIME       NULL,
    [SentTime]                           DATETIME       NULL,
    [Subject]                            VARCHAR (4000) NULL,
    [EmailName]                          VARCHAR (4000) NULL,
    [TriggeredSendExternalKey]           VARCHAR (4000) NULL,
    [SendDefinitionExternalKey]          VARCHAR (4000) NULL,
    [JobStatus]                          VARCHAR (4000) NULL,
    [PreviewURL]                         VARCHAR (4000) NULL,
    [IsMultipart]                        VARCHAR (4000) NULL,
    [Additional]                         VARCHAR (4000) NULL,
    [jan_one]                            DATETIME       NULL,
    [dv_load_date_time]                  DATETIME       NOT NULL,
    [dv_inserted_date_time]              DATETIME       NOT NULL,
    [dv_insert_user]                     VARCHAR (50)   NOT NULL,
    [dv_updated_date_time]               DATETIME       NULL,
    [dv_update_user]                     VARCHAR (50)   NULL,
    [dv_batch_id]                        BIGINT         NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));

