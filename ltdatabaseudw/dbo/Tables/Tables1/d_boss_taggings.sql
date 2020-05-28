CREATE TABLE [dbo].[d_boss_taggings] (
    [d_boss_taggings_id]    BIGINT        IDENTITY (1, 1) NOT NULL,
    [bk_hash]               CHAR (32)     NOT NULL,
    [taggings_id]           INT           NULL,
    [d_boss_tags_bk_hash]   CHAR (32)     NULL,
    [taggable_id]           INT           NULL,
    [taggable_type]         VARCHAR (255) NULL,
    [p_boss_taggings_id]    BIGINT        NOT NULL,
    [deleted_flag]          INT           NULL,
    [dv_load_date_time]     DATETIME      NULL,
    [dv_load_end_date_time] DATETIME      NULL,
    [dv_batch_id]           BIGINT        NOT NULL,
    [dv_inserted_date_time] DATETIME      NOT NULL,
    [dv_insert_user]        VARCHAR (50)  NOT NULL,
    [dv_updated_date_time]  DATETIME      NULL,
    [dv_update_user]        VARCHAR (50)  NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));


GO
CREATE NONCLUSTERED INDEX [ix_dv_batch_id]
    ON [dbo].[d_boss_taggings]([dv_batch_id] ASC);

