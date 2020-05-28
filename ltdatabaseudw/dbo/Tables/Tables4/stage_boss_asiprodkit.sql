CREATE TABLE [dbo].[stage_boss_asiprodkit] (
    [stage_boss_asiprodkit_id] BIGINT    NOT NULL,
    [parent_upc]               CHAR (15) NULL,
    [child_upc]                CHAR (15) NULL,
    [sort_order]               INT       NULL,
    [duration]                 INT       NULL,
    [jan_one]                  DATETIME  NULL,
    [dv_batch_id]              BIGINT    NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

