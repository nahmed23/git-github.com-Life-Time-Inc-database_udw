CREATE TABLE [dbo].[s_boss_taggings] (
    [s_boss_taggings_id]    BIGINT        IDENTITY (1, 1) NOT NULL,
    [bk_hash]               CHAR (32)     NOT NULL,
    [taggings_id]           INT           NULL,
    [taggable_type]         VARCHAR (255) NULL,
    [jan_one]               DATETIME      NULL,
    [dv_load_date_time]     DATETIME      NOT NULL,
    [dv_r_load_source_id]   BIGINT        NOT NULL,
    [dv_inserted_date_time] DATETIME      NOT NULL,
    [dv_insert_user]        VARCHAR (50)  NOT NULL,
    [dv_updated_date_time]  DATETIME      NULL,
    [dv_update_user]        VARCHAR (50)  NULL,
    [dv_hash]               CHAR (32)     NOT NULL,
    [dv_batch_id]           BIGINT        NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));


GO
CREATE CLUSTERED INDEX [ci_s_boss_taggings]
    ON [dbo].[s_boss_taggings]([bk_hash] ASC, [s_boss_taggings_id] ASC);


GO
CREATE NONCLUSTERED INDEX [ix_dv_batch_id]
    ON [dbo].[s_boss_taggings]([dv_batch_id] ASC);

