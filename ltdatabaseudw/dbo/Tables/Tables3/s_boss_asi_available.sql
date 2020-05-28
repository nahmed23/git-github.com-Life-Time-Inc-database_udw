CREATE TABLE [dbo].[s_boss_asi_available] (
    [s_boss_asi_available_id] BIGINT       IDENTITY (1, 1) NOT NULL,
    [bk_hash]                 CHAR (32)    NOT NULL,
    [club]                    INT          NULL,
    [resource_id]             INT          NULL,
    [start_time]              DATETIME     NULL,
    [end_time]                DATETIME     NULL,
    [schedule_type]           CHAR (1)     NULL,
    [dv_load_date_time]       DATETIME     NOT NULL,
    [dv_batch_id]             BIGINT       NOT NULL,
    [dv_r_load_source_id]     BIGINT       NOT NULL,
    [dv_inserted_date_time]   DATETIME     NOT NULL,
    [dv_insert_user]          VARCHAR (50) NOT NULL,
    [dv_updated_date_time]    DATETIME     NULL,
    [dv_update_user]          VARCHAR (50) NULL,
    [dv_hash]                 CHAR (32)    NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));


GO
CREATE CLUSTERED INDEX [ci_s_boss_asi_available]
    ON [dbo].[s_boss_asi_available]([bk_hash] ASC, [s_boss_asi_available_id] ASC);


GO
CREATE NONCLUSTERED INDEX [ix_dv_batch_id]
    ON [dbo].[s_boss_asi_available]([dv_batch_id] ASC);

