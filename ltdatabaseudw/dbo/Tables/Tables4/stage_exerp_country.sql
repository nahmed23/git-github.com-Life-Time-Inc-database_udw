CREATE TABLE [dbo].[stage_exerp_country] (
    [stage_exerp_country_id]   BIGINT         NOT NULL,
    [id]                       VARCHAR (4000) NULL,
    [name]                     VARCHAR (4000) NULL,
    [timezone]                 VARCHAR (4000) NULL,
    [dummy_modified_date_time] DATETIME       NULL,
    [dv_batch_id]              BIGINT         NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

