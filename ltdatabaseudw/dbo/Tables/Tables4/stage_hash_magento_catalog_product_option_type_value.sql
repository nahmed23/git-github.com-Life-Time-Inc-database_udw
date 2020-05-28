CREATE TABLE [dbo].[stage_hash_magento_catalog_product_option_type_value] (
    [stage_hash_magento_catalog_product_option_type_value_id] BIGINT       IDENTITY (1, 1) NOT NULL,
    [bk_hash]                                                 CHAR (32)    NOT NULL,
    [option_type_id]                                          INT          NULL,
    [option_id]                                               INT          NULL,
    [sku]                                                     VARCHAR (64) NULL,
    [sort_order]                                              INT          NULL,
    [dummy_modified_date_time]                                DATETIME     NULL,
    [dv_load_date_time]                                       DATETIME     NOT NULL,
    [dv_batch_id]                                             BIGINT       NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

