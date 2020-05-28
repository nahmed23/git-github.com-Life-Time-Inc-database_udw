CREATE TABLE [dbo].[stage_magento_sales_payment_transaction] (
    [stage_magento_sales_payment_transaction_id] BIGINT        NOT NULL,
    [transaction_id]                             INT           NULL,
    [parent_id]                                  INT           NULL,
    [order_id]                                   INT           NULL,
    [payment_id]                                 INT           NULL,
    [txn_id]                                     VARCHAR (100) NULL,
    [parent_txn_id]                              VARCHAR (100) NULL,
    [txn_type]                                   VARCHAR (15)  NULL,
    [is_closed]                                  INT           NULL,
    [created_at]                                 DATETIME      NULL,
    [dv_batch_id]                                BIGINT        NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

