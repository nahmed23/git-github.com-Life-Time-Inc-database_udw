CREATE TABLE [dbo].[stage_boss_taggings] (
    [stage_boss_taggings_id] BIGINT        NOT NULL,
    [id]                     INT           NULL,
    [tag_id]                 INT           NULL,
    [taggable_type]          VARCHAR (255) NULL,
    [taggable_id]            INT           NULL,
    [jan_one]                DATETIME      NULL,
    [dv_batch_id]            BIGINT        NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

