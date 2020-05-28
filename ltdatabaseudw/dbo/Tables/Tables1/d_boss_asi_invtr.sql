CREATE TABLE [dbo].[d_boss_asi_invtr] (
    [d_boss_asi_invtr_id]        BIGINT       IDENTITY (1, 1) NOT NULL,
    [bk_hash]                    CHAR (32)    NOT NULL,
    [invtr_upc_code]             CHAR (15)    NULL,
    [color]                      CHAR (8)     NULL,
    [created_dim_date_key]       CHAR (8)     NULL,
    [d_boss_asi_class_r_bk_hash] CHAR (32)    NULL,
    [d_boss_asi_color_r_bk_hash] CHAR (32)    NULL,
    [d_boss_asi_dept_m_bk_hash]  CHAR (32)    NULL,
    [d_boss_asi_size_r_bk_hash]  CHAR (32)    NULL,
    [dim_boss_product_key]       CHAR (32)    NULL,
    [dim_mms_product_key]        CHAR (32)    NULL,
    [display_flag]               CHAR (1)     NULL,
    [invtr_category_id]          INT          NULL,
    [invtr_id]                   INT          NULL,
    [last_sold_dim_date_key]     CHAR (8)     NULL,
    [product_description]        CHAR (50)    NULL,
    [size]                       CHAR (8)     NULL,
    [sku]                        CHAR (18)    NULL,
    [style]                      CHAR (8)     NULL,
    [p_boss_asi_invtr_id]        BIGINT       NOT NULL,
    [deleted_flag]               INT          NULL,
    [dv_load_date_time]          DATETIME     NULL,
    [dv_load_end_date_time]      DATETIME     NULL,
    [dv_batch_id]                BIGINT       NOT NULL,
    [dv_inserted_date_time]      DATETIME     NOT NULL,
    [dv_insert_user]             VARCHAR (50) NOT NULL,
    [dv_updated_date_time]       DATETIME     NULL,
    [dv_update_user]             VARCHAR (50) NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));


GO
CREATE NONCLUSTERED INDEX [ix_dv_batch_id]
    ON [dbo].[d_boss_asi_invtr]([dv_batch_id] ASC);

