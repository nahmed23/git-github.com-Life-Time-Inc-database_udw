CREATE TABLE [dbo].[stage_exerp_campaign] (
    [stage_exerp_campaign_id]  BIGINT         NOT NULL,
    [id]                       VARCHAR (4000) NULL,
    [state]                    VARCHAR (4000) NULL,
    [type]                     VARCHAR (4000) NULL,
    [name]                     VARCHAR (4000) NULL,
    [start_date]               DATETIME       NULL,
    [end_date]                 DATETIME       NULL,
    [campaign_codes_type]      VARCHAR (4000) NULL,
    [dummy_modified_date_time] DATETIME       NULL,
    [dv_batch_id]              BIGINT         NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

