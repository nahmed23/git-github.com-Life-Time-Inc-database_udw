CREATE TABLE [dbo].[l_boss_asi_club_res] (
    [l_boss_asi_club_res_id] BIGINT       IDENTITY (1, 1) NOT NULL,
    [bk_hash]                CHAR (32)    NOT NULL,
    [club]                   INT          NULL,
    [resource_id]            INT          NULL,
    [default_upccode]        CHAR (15)    NULL,
    [empl_id]                CHAR (6)     NULL,
    [display_seq]            SMALLINT     NULL,
    [resource_type_id]       SMALLINT     NULL,
    [employee_id]            INT          NULL,
    [dv_load_date_time]      DATETIME     NOT NULL,
    [dv_r_load_source_id]    BIGINT       NOT NULL,
    [dv_inserted_date_time]  DATETIME     NOT NULL,
    [dv_insert_user]         VARCHAR (50) NOT NULL,
    [dv_updated_date_time]   DATETIME     NULL,
    [dv_update_user]         VARCHAR (50) NULL,
    [dv_hash]                CHAR (32)    NOT NULL,
    [dv_batch_id]            BIGINT       NOT NULL
)
WITH (CLUSTERED INDEX([bk_hash]), DISTRIBUTION = HASH([bk_hash]));


GO
CREATE NONCLUSTERED INDEX [ix_dv_batch_id]
    ON [dbo].[l_boss_asi_club_res]([dv_batch_id] ASC);

