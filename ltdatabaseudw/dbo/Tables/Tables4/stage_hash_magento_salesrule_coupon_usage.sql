CREATE TABLE [dbo].[stage_hash_magento_salesrule_coupon_usage] (
    [stage_hash_magento_salesrule_coupon_usage_id] BIGINT    IDENTITY (1, 1) NOT NULL,
    [bk_hash]                                      CHAR (32) NOT NULL,
    [coupon_id]                                    INT       NULL,
    [customer_id]                                  INT       NULL,
    [times_used]                                   INT       NULL,
    [dummy_modified_date_time]                     DATETIME  NULL,
    [dv_load_date_time]                            DATETIME  NOT NULL,
    [dv_batch_id]                                  BIGINT    NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

