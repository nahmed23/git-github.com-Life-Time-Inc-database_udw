CREATE TABLE [dbo].[stage_boss_product_format] (
    [stage_boss_product_format_id] BIGINT        NOT NULL,
    [id]                           INT           NULL,
    [short_desc]                   CHAR (15)     NULL,
    [long_desc]                    CHAR (50)     NULL,
    [help_text]                    VARCHAR (240) NULL,
    [jan_one]                      DATETIME      NULL,
    [dv_batch_id]                  BIGINT        NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

