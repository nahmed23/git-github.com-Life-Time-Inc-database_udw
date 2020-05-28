CREATE TABLE [dbo].[stage_sfmc_content_send_log] (
    [stage_sfmc_content_send_log_id] BIGINT         NOT NULL,
    [JobID]                          VARCHAR (4000) NULL,
    [Member_ID]                      VARCHAR (4000) NULL,
    [ContentGUID]                    VARCHAR (4000) NULL,
    [IsTestSend]                     VARCHAR (4000) NULL,
    [SubscriberKey]                  VARCHAR (4000) NULL,
    [InsertDateTime]                 DATETIME       NULL,
    [dv_batch_id]                    BIGINT         NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

