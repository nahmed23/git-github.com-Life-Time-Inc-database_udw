CREATE TABLE [dbo].[d_boss_asi_prod_kit] (
    [d_boss_asi_prod_kit_id]      BIGINT       IDENTITY (1, 1) NOT NULL,
    [bk_hash]                     CHAR (32)    NOT NULL,
    [parent_upc]                  CHAR (15)    NULL,
    [child_upc]                   CHAR (15)    NULL,
    [child_dim_boss_product_key]  CHAR (32)    NULL,
    [duration]                    INT          NULL,
    [parent_dim_boss_product_key] CHAR (32)    NULL,
    [sort_order]                  INT          NULL,
    [p_boss_asi_prod_kit_id]      BIGINT       NOT NULL,
    [deleted_flag]                INT          NULL,
    [dv_load_date_time]           DATETIME     NULL,
    [dv_load_end_date_time]       DATETIME     NULL,
    [dv_batch_id]                 BIGINT       NOT NULL,
    [dv_inserted_date_time]       DATETIME     NOT NULL,
    [dv_insert_user]              VARCHAR (50) NOT NULL,
    [dv_updated_date_time]        DATETIME     NULL,
    [dv_update_user]              VARCHAR (50) NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));


GO
CREATE NONCLUSTERED INDEX [ix_dv_batch_id]
    ON [dbo].[d_boss_asi_prod_kit]([dv_batch_id] ASC);

