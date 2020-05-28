CREATE TABLE [dbo].[stage_magento_catalog_product_option_type_price] (
    [stage_magento_catalog_product_option_type_price_id] BIGINT          NOT NULL,
    [option_type_price_id]                               INT             NULL,
    [option_type_id]                                     INT             NULL,
    [store_id]                                           INT             NULL,
    [price]                                              DECIMAL (12, 4) NULL,
    [price_type]                                         VARCHAR (7)     NULL,
    [dummy_modified_date_time]                           DATETIME        NULL,
    [dv_batch_id]                                        BIGINT          NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

