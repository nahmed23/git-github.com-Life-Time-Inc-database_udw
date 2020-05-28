CREATE TABLE [dbo].[stage_magento_salesrule_customer] (
    [stage_magento_salesrule_customer_id] BIGINT   NOT NULL,
    [rule_customer_id]                    INT      NULL,
    [rule_id]                             INT      NULL,
    [customer_id]                         INT      NULL,
    [times_used]                          INT      NULL,
    [dummy_modified_date_time]            DATETIME NULL,
    [dv_batch_id]                         BIGINT   NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

