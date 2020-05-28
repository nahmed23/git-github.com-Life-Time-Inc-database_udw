CREATE TABLE [dbo].[s_boss_participation] (
    [s_boss_participation_id] BIGINT       IDENTITY (1, 1) NOT NULL,
    [bk_hash]                 CHAR (32)    NOT NULL,
    [participation_date]      DATETIME     NULL,
    [no_participants]         INT          NULL,
    [comment]                 VARCHAR (80) NULL,
    [no_non_mbr]              INT          NULL,
    [updated_at]              DATETIME     NULL,
    [created_at]              DATETIME     NULL,
    [participation_id]        INT          NULL,
    [system_count]            INT          NULL,
    [mod_count]               INT          NULL,
    [dv_load_date_time]       DATETIME     NOT NULL,
    [dv_r_load_source_id]     BIGINT       NOT NULL,
    [dv_inserted_date_time]   DATETIME     NOT NULL,
    [dv_insert_user]          VARCHAR (50) NOT NULL,
    [dv_updated_date_time]    DATETIME     NULL,
    [dv_update_user]          VARCHAR (50) NULL,
    [dv_hash]                 CHAR (32)    NOT NULL,
    [dv_deleted]              BIT          DEFAULT ((0)) NOT NULL,
    [dv_batch_id]             BIGINT       NOT NULL
)
WITH (CLUSTERED INDEX([bk_hash]), DISTRIBUTION = HASH([bk_hash]));


GO
CREATE NONCLUSTERED INDEX [ix_dv_batch_id]
    ON [dbo].[s_boss_participation]([dv_batch_id] ASC);

