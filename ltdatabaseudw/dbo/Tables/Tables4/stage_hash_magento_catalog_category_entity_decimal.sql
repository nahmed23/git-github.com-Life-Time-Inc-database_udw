CREATE TABLE [dbo].[stage_hash_magento_catalog_category_entity_decimal] (
    [stage_hash_magento_catalog_category_entity_decimal_id] BIGINT          IDENTITY (1, 1) NOT NULL,
    [bk_hash]                                               CHAR (32)       NOT NULL,
    [value_id]                                              INT             NULL,
    [attribute_id]                                          INT             NULL,
    [store_id]                                              INT             NULL,
    [row_id]                                                INT             NULL,
    [value]                                                 DECIMAL (12, 4) NULL,
    [dummy_modified_date_time]                              DATETIME        NULL,
    [dv_load_date_time]                                     DATETIME        NOT NULL,
    [dv_batch_id]                                           BIGINT          NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

