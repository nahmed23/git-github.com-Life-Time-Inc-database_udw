CREATE TABLE [dbo].[stage_boss_interest] (
    [stage_boss_interest_id]   BIGINT    NOT NULL,
    [id]                       INT       NULL,
    [short_desc]               CHAR (15) NULL,
    [long_desc]                CHAR (50) NULL,
    [dummy_modified_date_time] DATETIME  NULL,
    [dv_batch_id]              BIGINT    NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

