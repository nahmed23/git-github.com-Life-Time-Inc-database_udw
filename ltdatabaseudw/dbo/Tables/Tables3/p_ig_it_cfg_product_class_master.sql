CREATE TABLE [dbo].[p_ig_it_cfg_product_class_master] (
    [p_ig_it_cfg_product_class_master_id]  BIGINT       IDENTITY (1, 1) NOT NULL,
    [bk_hash]                              CHAR (32)    NOT NULL,
    [ent_id]                               INT          NULL,
    [prod_class_id]                        INT          NULL,
    [l_ig_it_cfg_product_class_master_id]  BIGINT       NULL,
    [s_ig_it_cfg_product_class_master_id]  BIGINT       NULL,
    [dv_load_date_time]                    DATETIME     NOT NULL,
    [dv_load_end_date_time]                DATETIME     NOT NULL,
    [dv_greatest_satellite_date_time]      DATETIME     NULL,
    [dv_next_greatest_satellite_date_time] DATETIME     NULL,
    [dv_first_in_key_series]               INT          NULL,
    [dv_inserted_date_time]                DATETIME     NOT NULL,
    [dv_insert_user]                       VARCHAR (50) NOT NULL,
    [dv_updated_date_time]                 DATETIME     NULL,
    [dv_update_user]                       VARCHAR (50) NULL,
    [dv_batch_id]                          BIGINT       NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));


GO
CREATE CLUSTERED INDEX [ci_p_ig_it_cfg_product_class_master]
    ON [dbo].[p_ig_it_cfg_product_class_master]([bk_hash] ASC, [p_ig_it_cfg_product_class_master_id] ASC);


GO
CREATE NONCLUSTERED INDEX [ix_dv_batch_id]
    ON [dbo].[p_ig_it_cfg_product_class_master]([dv_batch_id] ASC);

