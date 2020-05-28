CREATE TABLE [dbo].[l_ig_it_cfg_product_class_master] (
    [l_ig_it_cfg_product_class_master_id] BIGINT       IDENTITY (1, 1) NOT NULL,
    [bk_hash]                             CHAR (32)    NOT NULL,
    [ent_id]                              INT          NULL,
    [prod_class_id]                       INT          NULL,
    [default_rev_cat_id]                  INT          NULL,
    [default_tax_grp_id]                  INT          NULL,
    [default_sec_id]                      INT          NULL,
    [default_menu_item_group_id]          INT          NULL,
    [default_rpt_cat_id]                  INT          NULL,
    [store_id]                            INT          NULL,
    [dv_load_date_time]                   DATETIME     NOT NULL,
    [dv_r_load_source_id]                 BIGINT       NOT NULL,
    [dv_inserted_date_time]               DATETIME     NOT NULL,
    [dv_insert_user]                      VARCHAR (50) NOT NULL,
    [dv_updated_date_time]                DATETIME     NULL,
    [dv_update_user]                      VARCHAR (50) NULL,
    [dv_hash]                             CHAR (32)    NOT NULL,
    [dv_batch_id]                         BIGINT       NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));


GO
CREATE CLUSTERED INDEX [ci_l_ig_it_cfg_product_class_master]
    ON [dbo].[l_ig_it_cfg_product_class_master]([bk_hash] ASC, [l_ig_it_cfg_product_class_master_id] ASC);


GO
CREATE NONCLUSTERED INDEX [ix_dv_batch_id]
    ON [dbo].[l_ig_it_cfg_product_class_master]([dv_batch_id] ASC);

