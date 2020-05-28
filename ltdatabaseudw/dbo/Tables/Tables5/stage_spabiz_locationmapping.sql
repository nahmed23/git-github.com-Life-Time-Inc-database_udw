CREATE TABLE [dbo].[stage_spabiz_locationmapping] (
    [stage_spabiz_locationmapping_id] BIGINT         NOT NULL,
    [NAME]                            VARCHAR (4000) NULL,
    [Spabiz_STORE_NUMBER]             BIGINT         NULL,
    [Workday_Id]                      BIGINT         NULL,
    [jan_one]                         DATETIME       NULL,
    [dv_batch_id]                     BIGINT         NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

