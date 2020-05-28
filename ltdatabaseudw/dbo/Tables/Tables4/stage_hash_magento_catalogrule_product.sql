CREATE TABLE [dbo].[stage_hash_magento_catalogrule_product] (
    [stage_hash_magento_catalogrule_product_id] BIGINT          IDENTITY (1, 1) NOT NULL,
    [bk_hash]                                   CHAR (32)       NOT NULL,
    [rule_product_id]                           INT             NULL,
    [rule_id]                                   INT             NULL,
    [from_time]                                 INT             NULL,
    [to_time]                                   INT             NULL,
    [customer_group_id]                         INT             NULL,
    [product_id]                                INT             NULL,
    [action_operator]                           VARCHAR (10)    NULL,
    [action_amount]                             DECIMAL (12, 4) NULL,
    [action_stop]                               INT             NULL,
    [sort_order]                                INT             NULL,
    [website_id]                                INT             NULL,
    [dummy_modified_date_time]                  DATETIME        NULL,
    [dv_load_date_time]                         DATETIME        NOT NULL,
    [dv_batch_id]                               BIGINT          NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

