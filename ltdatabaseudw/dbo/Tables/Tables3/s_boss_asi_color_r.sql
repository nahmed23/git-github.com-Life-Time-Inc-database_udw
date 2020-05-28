CREATE TABLE [dbo].[s_boss_asi_color_r] (
    [s_boss_asi_color_r_id] BIGINT       IDENTITY (1, 1) NOT NULL,
    [bk_hash]               CHAR (32)    NOT NULL,
    [color_r_dept]          INT          NULL,
    [color_r_class]         INT          NULL,
    [color_r_code]          CHAR (8)     NULL,
    [color_r_desc]          CHAR (30)    NULL,
    [color_r_seq]           SMALLINT     NULL,
    [jan_one]               DATETIME     NULL,
    [dv_load_date_time]     DATETIME     NOT NULL,
    [dv_r_load_source_id]   BIGINT       NOT NULL,
    [dv_inserted_date_time] DATETIME     NOT NULL,
    [dv_insert_user]        VARCHAR (50) NOT NULL,
    [dv_updated_date_time]  DATETIME     NULL,
    [dv_update_user]        VARCHAR (50) NULL,
    [dv_hash]               CHAR (32)    NOT NULL,
    [dv_batch_id]           BIGINT       NOT NULL
)
WITH (CLUSTERED INDEX([bk_hash]), DISTRIBUTION = HASH([bk_hash]));


GO
CREATE NONCLUSTERED INDEX [ix_dv_batch_id]
    ON [dbo].[s_boss_asi_color_r]([dv_batch_id] ASC);

