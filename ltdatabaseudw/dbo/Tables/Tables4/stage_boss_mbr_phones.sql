CREATE TABLE [dbo].[stage_boss_mbr_phones] (
    [stage_boss_mbr_phones_id] BIGINT      NOT NULL,
    [id]                       INT         NULL,
    [area_code]                VARCHAR (3) NULL,
    [number]                   VARCHAR (7) NULL,
    [ext]                      VARCHAR (5) NULL,
    [ph_type]                  VARCHAR (1) NULL,
    [contact_id]               INT         NULL,
    [created_at]               DATETIME    NULL,
    [updated_at]               DATETIME    NULL,
    [dv_batch_id]              BIGINT      NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

