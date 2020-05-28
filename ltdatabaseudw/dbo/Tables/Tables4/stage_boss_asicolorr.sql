CREATE TABLE [dbo].[stage_boss_asicolorr] (
    [stage_boss_asicolorr_id] BIGINT    NOT NULL,
    [colorr_dept]             INT       NULL,
    [colorr_class]            INT       NULL,
    [colorr_code]             CHAR (8)  NULL,
    [colorr_desc]             CHAR (30) NULL,
    [colorr_seq]              SMALLINT  NULL,
    [colorr_class_id]         INT       NULL,
    [id]                      INT       NULL,
    [jan_one]                 DATETIME  NULL,
    [dv_batch_id]             BIGINT    NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

