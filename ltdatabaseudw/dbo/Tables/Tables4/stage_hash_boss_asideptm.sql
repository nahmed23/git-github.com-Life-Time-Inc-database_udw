CREATE TABLE [dbo].[stage_hash_boss_asideptm] (
    [stage_hash_boss_asideptm_id] BIGINT       IDENTITY (1, 1) NOT NULL,
    [bk_hash]                     CHAR (32)    NOT NULL,
    [deptm_code]                  INT          NULL,
    [deptm_desc]                  CHAR (30)    NULL,
    [deptm_has_res]               CHAR (8)     NULL,
    [deptm_legacy_code]           INT          NULL,
    [deptm_created_at]            DATETIME     NULL,
    [deptm_updated_at]            DATETIME     NULL,
    [deptm_id]                    INT          NULL,
    [dv_load_date_time]           DATETIME     NOT NULL,
    [dv_inserted_date_time]       DATETIME     NOT NULL,
    [dv_insert_user]              VARCHAR (50) NOT NULL,
    [dv_updated_date_time]        DATETIME     NULL,
    [dv_update_user]              VARCHAR (50) NULL,
    [dv_batch_id]                 BIGINT       NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));

