CREATE TABLE [dbo].[stage_magento_salesrule_label] (
    [stage_magento_salesrule_label_id] BIGINT        NOT NULL,
    [label_id]                         INT           NULL,
    [rule_id]                          INT           NULL,
    [store_id]                         INT           NULL,
    [label]                            VARCHAR (255) NULL,
    [dummy_modified_date_time]         DATETIME      NULL,
    [dv_batch_id]                      BIGINT        NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

