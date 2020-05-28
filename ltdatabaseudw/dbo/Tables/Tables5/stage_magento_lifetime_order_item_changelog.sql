CREATE TABLE [dbo].[stage_magento_lifetime_order_item_changelog] (
    [stage_magento_lifetime_order_item_changelog_id] BIGINT        NOT NULL,
    [entity_id]                                      INT           NULL,
    [item_id]                                        INT           NULL,
    [mms_id]                                         VARCHAR (32)  NULL,
    [status]                                         INT           NULL,
    [transaction_type]                               VARCHAR (255) NULL,
    [transaction_id]                                 VARCHAR (255) NULL,
    [dummy_modified_date_time]                       DATETIME      NULL,
    [dv_batch_id]                                    BIGINT        NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

