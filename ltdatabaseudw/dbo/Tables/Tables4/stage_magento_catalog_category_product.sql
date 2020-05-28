CREATE TABLE [dbo].[stage_magento_catalog_category_product] (
    [stage_magento_catalog_category_product_id] BIGINT   NOT NULL,
    [entity_id]                                 INT      NULL,
    [category_id]                               INT      NULL,
    [product_id]                                INT      NULL,
    [position]                                  INT      NULL,
    [dummy_modified_date_time]                  DATETIME NULL,
    [dv_batch_id]                               BIGINT   NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

