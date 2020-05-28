CREATE TABLE [dbo].[stage_spabiz_vendormapping] (
    [stage_spabiz_vendormapping_id] BIGINT   NOT NULL,
    [idvendormapping]               BIGINT   NULL,
    [spabiz_vendordatabaseid]       BIGINT   NULL,
    [workday_supplierid]            BIGINT   NULL,
    [jan_one]                       DATETIME NULL,
    [dv_batch_id]                   BIGINT   NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

