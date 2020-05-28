CREATE TABLE [dbo].[stage_boss_asisizer] (
    [stage_boss_asisizer_id] BIGINT    NOT NULL,
    [sizer_dept]             INT       NULL,
    [sizer_class]            INT       NULL,
    [sizer_code]             CHAR (8)  NULL,
    [sizer_desc]             CHAR (30) NULL,
    [sizer_seq]              SMALLINT  NULL,
    [sizer_class_id]         INT       NULL,
    [id]                     INT       NULL,
    [jan_one]                DATETIME  NULL,
    [dv_batch_id]            BIGINT    NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

