CREATE TABLE [dbo].[d_magento_catalog_category_product] (
    [d_magento_catalog_category_product_id] BIGINT       IDENTITY (1, 1) NOT NULL,
    [bk_hash]                               CHAR (32)    NOT NULL,
    [entity_id]                             INT          NULL,
    [category_id]                           INT          NULL,
    [product_id]                            INT          NULL,
    [dim_magento_category_key]              VARCHAR (32) NULL,
    [dim_magento_product_key]               VARCHAR (32) NULL,
    [position]                              INT          NULL,
    [p_magento_catalog_category_product_id] BIGINT       NOT NULL,
    [deleted_flag]                          INT          NULL,
    [dv_load_date_time]                     DATETIME     NULL,
    [dv_load_end_date_time]                 DATETIME     NULL,
    [dv_batch_id]                           BIGINT       NOT NULL,
    [dv_inserted_date_time]                 DATETIME     NOT NULL,
    [dv_insert_user]                        VARCHAR (50) NOT NULL,
    [dv_updated_date_time]                  DATETIME     NULL,
    [dv_update_user]                        VARCHAR (50) NULL
)
WITH (CLUSTERED COLUMNSTORE INDEX, DISTRIBUTION = HASH([bk_hash]));

