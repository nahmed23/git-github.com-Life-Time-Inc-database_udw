CREATE TABLE [dbo].[stage_hash_magento_salesrule_customer] (
    [stage_hash_magento_salesrule_customer_id] BIGINT    IDENTITY (1, 1) NOT NULL,
    [bk_hash]                                  CHAR (32) NOT NULL,
    [rule_customer_id]                         INT       NULL,
    [rule_id]                                  INT       NULL,
    [customer_id]                              INT       NULL,
    [times_used]                               INT       NULL,
    [dummy_modified_date_time]                 DATETIME  NULL,
    [dv_load_date_time]                        DATETIME  NOT NULL,
    [dv_batch_id]                              BIGINT    NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));

