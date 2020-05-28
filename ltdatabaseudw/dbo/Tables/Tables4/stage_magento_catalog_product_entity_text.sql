CREATE TABLE [dbo].[stage_magento_catalog_product_entity_text] (
    [stage_magento_catalog_product_entity_text_id] BIGINT         NOT NULL,
    [value_id]                                     INT            NULL,
    [attribute_id]                                 INT            NULL,
    [store_id]                                     INT            NULL,
    [row_id]                                       INT            NULL,
    [value]                                        VARCHAR (8000) NULL,
    [dummy_modified_date_time]                     DATETIME       NULL,
    [dv_batch_id]                                  BIGINT         NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

