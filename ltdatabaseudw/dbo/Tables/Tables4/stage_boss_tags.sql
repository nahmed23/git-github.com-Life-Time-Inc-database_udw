CREATE TABLE [dbo].[stage_boss_tags] (
    [stage_boss_tags_id] BIGINT        NOT NULL,
    [id]                 INT           NULL,
    [name]               VARCHAR (255) NULL,
    [kind]               VARCHAR (255) NULL,
    [jan_one]            DATETIME      NULL,
    [dv_batch_id]        BIGINT        NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

