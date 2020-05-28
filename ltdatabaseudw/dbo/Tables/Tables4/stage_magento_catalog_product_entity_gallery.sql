CREATE TABLE [dbo].[stage_magento_catalog_product_entity_gallery] (
    [stage_magento_catalog_product_entity_gallery_id] BIGINT        NOT NULL,
    [value_id]                                        INT           NULL,
    [attribute_id]                                    INT           NULL,
    [store_id]                                        INT           NULL,
    [row_id]                                          INT           NULL,
    [position]                                        INT           NULL,
    [value]                                           VARCHAR (255) NULL,
    [dummy_modified_date_time]                        DATETIME      NULL,
    [dv_batch_id]                                     BIGINT        NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

