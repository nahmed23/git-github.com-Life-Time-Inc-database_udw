CREATE TABLE [dbo].[d_magento_catalog_product_option] (
    [d_magento_catalog_product_option_id]        BIGINT       IDENTITY (1, 1) NOT NULL,
    [bk_hash]                                    CHAR (32)    NOT NULL,
    [option_id]                                  INT          NULL,
    [catalog_product_option_type]                VARCHAR (50) NULL,
    [d_magento_catalog_category_product_bk_hash] CHAR (32)    NULL,
    [file_extension]                             VARCHAR (50) NULL,
    [image_size_x]                               INT          NULL,
    [image_size_y]                               INT          NULL,
    [is_requires_flag]                           CHAR (1)     NULL,
    [max_characters]                             INT          NULL,
    [sku]                                        VARCHAR (64) NULL,
    [sort_order]                                 INT          NULL,
    [p_magento_catalog_product_option_id]        BIGINT       NOT NULL,
    [deleted_flag]                               INT          NULL,
    [dv_load_date_time]                          DATETIME     NULL,
    [dv_load_end_date_time]                      DATETIME     NULL,
    [dv_batch_id]                                BIGINT       NOT NULL,
    [dv_inserted_date_time]                      DATETIME     NOT NULL,
    [dv_insert_user]                             VARCHAR (50) NOT NULL,
    [dv_updated_date_time]                       DATETIME     NULL,
    [dv_update_user]                             VARCHAR (50) NULL
)
WITH (CLUSTERED COLUMNSTORE INDEX, DISTRIBUTION = HASH([bk_hash]));

