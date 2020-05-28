CREATE TABLE [dbo].[stage_magento_catalog_product_bundle_selection_price] (
    [stage_magento_catalog_product_bundle_selection_price_id] BIGINT          NOT NULL,
    [selection_id]                                            INT             NULL,
    [parent_product_id]                                       INT             NULL,
    [website_id]                                              INT             NULL,
    [selection_price_type]                                    INT             NULL,
    [selection_price_value]                                   DECIMAL (12, 4) NULL,
    [dummy_modified_date_time]                                DATETIME        NULL,
    [dv_batch_id]                                             BIGINT          NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

