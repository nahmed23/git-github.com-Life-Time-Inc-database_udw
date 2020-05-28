CREATE TABLE [dbo].[l_boss_employees] (
    [l_boss_employees_id]   BIGINT       IDENTITY (1, 1) NOT NULL,
    [bk_hash]               CHAR (32)    NOT NULL,
    [interestID]            INT          NULL,
    [home_club]             INT          NULL,
    [roleID]                INT          NULL,
    [employee_id]           INT          NULL,
    [id]                    INT          NULL,
    [member_ID]             CHAR (10)    NULL,
    [dv_load_date_time]     DATETIME     NOT NULL,
    [dv_r_load_source_id]   BIGINT       NOT NULL,
    [dv_inserted_date_time] DATETIME     NOT NULL,
    [dv_insert_user]        VARCHAR (50) NOT NULL,
    [dv_updated_date_time]  DATETIME     NULL,
    [dv_update_user]        VARCHAR (50) NULL,
    [dv_hash]               CHAR (32)    NOT NULL,
    [dv_batch_id]           BIGINT       NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));


GO
CREATE CLUSTERED INDEX [ci_l_boss_employees]
    ON [dbo].[l_boss_employees]([bk_hash] ASC, [l_boss_employees_id] ASC);


GO
CREATE NONCLUSTERED INDEX [ix_dv_batch_id]
    ON [dbo].[l_boss_employees]([dv_batch_id] ASC);

