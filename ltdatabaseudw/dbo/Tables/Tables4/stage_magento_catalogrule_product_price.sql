CREATE TABLE [dbo].[stage_magento_catalogrule_product_price] (
    [stage_magento_catalogrule_product_price_id] BIGINT          NOT NULL,
    [rule_product_price_id]                      INT             NULL,
    [rule_date]                                  DATETIME        NULL,
    [customer_group_id]                          INT             NULL,
    [product_id]                                 INT             NULL,
    [rule_price]                                 DECIMAL (12, 4) NULL,
    [website_id]                                 INT             NULL,
    [latest_start_date]                          DATETIME        NULL,
    [earliest_end_date]                          DATETIME        NULL,
    [dv_batch_id]                                BIGINT          NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

