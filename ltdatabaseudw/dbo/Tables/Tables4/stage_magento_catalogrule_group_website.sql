CREATE TABLE [dbo].[stage_magento_catalogrule_group_website] (
    [stage_magento_catalogrule_group_website_id] BIGINT   NOT NULL,
    [rule_id]                                    INT      NULL,
    [customer_group_id]                          INT      NULL,
    [website_id]                                 INT      NULL,
    [dummy_modified_date_time]                   DATETIME NULL,
    [dv_batch_id]                                BIGINT   NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

