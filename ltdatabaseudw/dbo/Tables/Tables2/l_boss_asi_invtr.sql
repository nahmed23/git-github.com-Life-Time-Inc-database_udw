CREATE TABLE [dbo].[l_boss_asi_invtr] (
    [l_boss_asi_invtr_id]   BIGINT       IDENTITY (1, 1) NOT NULL,
    [bk_hash]               CHAR (32)    NOT NULL,
    [invtr_upc_code]        CHAR (15)    NULL,
    [invtr_dept]            INT          NULL,
    [invtr_class]           INT          NULL,
    [invtr_vendor]          CHAR (18)    NULL,
    [invtr_size]            CHAR (8)     NULL,
    [invtr_legacy_prod_id]  INT          NULL,
    [invtr_class_id]        INT          NULL,
    [invtr_vendor_prod_id]  CHAR (15)    NULL,
    [invtr_category_id]     INT          NULL,
    [invtr_can_reorder]     CHAR (1)     NULL,
    [waiver_file_id]        INT          NULL,
    [invtr_id]              INT          NULL,
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
    ON [dbo].[l_boss_asi_invtr]([dv_batch_id] ASC);

