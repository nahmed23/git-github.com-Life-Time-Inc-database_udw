CREATE TABLE [dbo].[d_boss_asi_color_r] (
    [d_boss_asi_color_r_id]     BIGINT       IDENTITY (1, 1) NOT NULL,
    [bk_hash]                   CHAR (32)    NOT NULL,
    [color_r_dept]              INT          NULL,
    [color_r_class]             INT          NULL,
    [color_r_code]              CHAR (8)     NULL,
    [product_hierarchy_level_3] CHAR (30)    NULL,
    [p_boss_asi_color_r_id]     BIGINT       NOT NULL,
    [deleted_flag]              INT          NULL,
    [dv_load_date_time]         DATETIME     NULL,
    [dv_load_end_date_time]     DATETIME     NULL,
    [dv_batch_id]               BIGINT       NOT NULL,
    [dv_inserted_date_time]     DATETIME     NOT NULL,
    [dv_insert_user]            VARCHAR (50) NOT NULL,
    [dv_updated_date_time]      DATETIME     NULL,
    [dv_update_user]            VARCHAR (50) NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));


GO
CREATE NONCLUSTERED INDEX [ix_dv_batch_id]
    ON [dbo].[d_boss_asi_color_r]([dv_batch_id] ASC);

