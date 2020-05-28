CREATE TABLE [dbo].[s_boss_asi_dept_m] (
    [s_boss_asi_dept_m_id]  BIGINT       IDENTITY (1, 1) NOT NULL,
    [bk_hash]               CHAR (32)    NOT NULL,
    [dept_m_code]           INT          NULL,
    [dept_m_desc]           CHAR (30)    NULL,
    [dept_m_has_res]        CHAR (8)     NULL,
    [dept_m_created_at]     DATETIME     NULL,
    [dept_m_updated_at]     DATETIME     NULL,
    [dv_load_date_time]     DATETIME     NOT NULL,
    [dv_batch_id]           BIGINT       NOT NULL,
    [dv_r_load_source_id]   BIGINT       NOT NULL,
    [dv_inserted_date_time] DATETIME     NOT NULL,
    [dv_insert_user]        VARCHAR (50) NOT NULL,
    [dv_updated_date_time]  DATETIME     NULL,
    [dv_update_user]        VARCHAR (50) NULL,
    [dv_hash]               CHAR (32)    NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));


GO
CREATE CLUSTERED INDEX [ci_s_boss_asi_dept_m]
    ON [dbo].[s_boss_asi_dept_m]([bk_hash] ASC, [s_boss_asi_dept_m_id] ASC);


GO
CREATE NONCLUSTERED INDEX [ix_dv_batch_id]
    ON [dbo].[s_boss_asi_dept_m]([dv_batch_id] ASC);

