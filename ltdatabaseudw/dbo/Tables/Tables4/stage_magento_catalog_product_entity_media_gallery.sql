CREATE TABLE [dbo].[stage_magento_catalog_product_entity_media_gallery] (
    [stage_magento_catalog_product_entity_media_gallery_id] BIGINT        NOT NULL,
    [value_id]                                              INT           NULL,
    [attribute_id]                                          INT           NULL,
    [value]                                                 VARCHAR (255) NULL,
    [media_type]                                            VARCHAR (32)  NULL,
    [disabled]                                              INT           NULL,
    [dummy_modified_date_time]                              DATETIME      NULL,
    [dv_batch_id]                                           BIGINT        NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

