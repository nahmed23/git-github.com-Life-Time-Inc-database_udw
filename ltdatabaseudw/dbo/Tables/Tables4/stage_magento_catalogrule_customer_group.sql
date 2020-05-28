CREATE TABLE [dbo].[stage_magento_catalogrule_customer_group] (
    [stage_magento_catalogrule_customer_group_id] BIGINT   NOT NULL,
    [row_id]                                      INT      NULL,
    [customer_group_id]                           INT      NULL,
    [dummy_modified_date_time]                    DATETIME NULL,
    [dv_batch_id]                                 BIGINT   NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

