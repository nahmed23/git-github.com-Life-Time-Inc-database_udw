CREATE TABLE [dbo].[stage_magento_catalog_product_entity_tier_price] (
    [stage_magento_catalog_product_entity_tier_price_id] BIGINT          NOT NULL,
    [value_id]                                           INT             NULL,
    [row_id]                                             INT             NULL,
    [all_groups]                                         INT             NULL,
    [customer_group_id]                                  INT             NULL,
    [qty]                                                DECIMAL (12, 4) NULL,
    [value]                                              DECIMAL (12, 4) NULL,
    [percentage_value]                                   DECIMAL (5, 2)  NULL,
    [website_id]                                         INT             NULL,
    [dummy_modified_date_time]                           DATETIME        NULL,
    [dv_batch_id]                                        BIGINT          NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

