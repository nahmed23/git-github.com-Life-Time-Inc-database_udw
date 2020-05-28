CREATE TABLE [dbo].[stage_hash_sfmc_content_send_log] (
    [stage_hash_sfmc_content_send_log_id] BIGINT         IDENTITY (1, 1) NOT NULL,
    [bk_hash]                             CHAR (32)      NOT NULL,
    [JobID]                               VARCHAR (4000) NULL,
    [Member_ID]                           VARCHAR (4000) NULL,
    [ContentGUID]                         VARCHAR (4000) NULL,
    [IsTestSend]                          VARCHAR (4000) NULL,
    [SubscriberKey]                       VARCHAR (4000) NULL,
    [InsertDateTime]                      DATETIME       NULL,
    [dv_load_date_time]                   DATETIME       NOT NULL,
    [dv_batch_id]                         BIGINT         NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));

