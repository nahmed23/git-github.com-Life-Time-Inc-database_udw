CREATE TABLE [dbo].[stage_magento_catalog_product_bundle_selection] (
    [stage_magento_catalog_product_bundle_selection_id] BIGINT          NOT NULL,
    [selection_id]                                      INT             NULL,
    [parent_product_id]                                 INT             NULL,
    [option_id]                                         INT             NULL,
    [product_id]                                        INT             NULL,
    [position]                                          INT             NULL,
    [is_default]                                        INT             NULL,
    [selection_price_type]                              INT             NULL,
    [selection_price_value]                             DECIMAL (12, 4) NULL,
    [selection_qty]                                     DECIMAL (12, 4) NULL,
    [selection_can_change_qty]                          INT             NULL,
    [dummy_modified_date_time]                          DATETIME        NULL,
    [dv_batch_id]                                       BIGINT          NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

