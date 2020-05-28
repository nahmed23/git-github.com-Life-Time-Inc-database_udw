CREATE TABLE [dbo].[stage_boss_asideptm] (
    [stage_boss_asideptm_id] BIGINT    NOT NULL,
    [deptm_code]             INT       NULL,
    [deptm_desc]             CHAR (30) NULL,
    [deptm_has_res]          CHAR (8)  NULL,
    [deptm_legacy_code]      INT       NULL,
    [deptm_created_at]       DATETIME  NULL,
    [deptm_updated_at]       DATETIME  NULL,
    [deptm_id]               INT       NULL,
    [dv_batch_id]            BIGINT    NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

