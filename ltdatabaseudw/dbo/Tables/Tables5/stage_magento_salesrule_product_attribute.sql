CREATE TABLE [dbo].[stage_magento_salesrule_product_attribute] (
    [stage_magento_salesrule_product_attribute_id] BIGINT   NOT NULL,
    [row_id]                                       INT      NULL,
    [website_id]                                   INT      NULL,
    [customer_group_id]                            INT      NULL,
    [attribute_id]                                 INT      NULL,
    [dummy_modified_date_time]                     DATETIME NULL,
    [dv_batch_id]                                  BIGINT   NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

