CREATE TABLE [dbo].[stage_hash_magento_catalogrule_product_price] (
    [stage_hash_magento_catalogrule_product_price_id] BIGINT          IDENTITY (1, 1) NOT NULL,
    [bk_hash]                                         CHAR (32)       NOT NULL,
    [rule_product_price_id]                           INT             NULL,
    [rule_date]                                       DATETIME        NULL,
    [customer_group_id]                               INT             NULL,
    [product_id]                                      INT             NULL,
    [rule_price]                                      DECIMAL (12, 4) NULL,
    [website_id]                                      INT             NULL,
    [latest_start_date]                               DATETIME        NULL,
    [earliest_end_date]                               DATETIME        NULL,
    [dv_load_date_time]                               DATETIME        NOT NULL,
    [dv_batch_id]                                     BIGINT          NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

