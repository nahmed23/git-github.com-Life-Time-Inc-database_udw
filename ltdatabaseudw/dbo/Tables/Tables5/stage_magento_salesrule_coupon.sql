CREATE TABLE [dbo].[stage_magento_salesrule_coupon] (
    [stage_magento_salesrule_coupon_id] BIGINT        NOT NULL,
    [coupon_id]                         INT           NULL,
    [rule_id]                           INT           NULL,
    [code]                              VARCHAR (255) NULL,
    [usage_limit]                       INT           NULL,
    [usage_per_customer]                INT           NULL,
    [times_used]                        INT           NULL,
    [expiration_date]                   DATETIME      NULL,
    [is_primary]                        INT           NULL,
    [created_at]                        DATETIME      NULL,
    [type]                              INT           NULL,
    [generated_by_dotmailer]            INT           NULL,
    [dummy_modified_date_time]          DATETIME      NULL,
    [dv_batch_id]                       BIGINT        NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

