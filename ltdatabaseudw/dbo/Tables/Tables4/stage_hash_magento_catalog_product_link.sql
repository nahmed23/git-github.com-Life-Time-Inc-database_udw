CREATE TABLE [dbo].[stage_hash_magento_catalog_product_link] (
    [stage_hash_magento_catalog_product_link_id] BIGINT    IDENTITY (1, 1) NOT NULL,
    [bk_hash]                                    CHAR (32) NOT NULL,
    [link_id]                                    INT       NULL,
    [product_id]                                 INT       NULL,
    [linked_product_id]                          INT       NULL,
    [link_type_id]                               INT       NULL,
    [dummy_modified_date_time]                   DATETIME  NULL,
    [dv_load_date_time]                          DATETIME  NOT NULL,
    [dv_batch_id]                                BIGINT    NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

