CREATE TABLE [dbo].[stage_magento_catalog_product_link] (
    [stage_magento_catalog_product_link_id] BIGINT   NOT NULL,
    [link_id]                               INT      NULL,
    [product_id]                            INT      NULL,
    [linked_product_id]                     INT      NULL,
    [link_type_id]                          INT      NULL,
    [dummy_modified_date_time]              DATETIME NULL,
    [dv_batch_id]                           BIGINT   NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

