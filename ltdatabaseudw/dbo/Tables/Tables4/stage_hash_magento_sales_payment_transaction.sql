CREATE TABLE [dbo].[stage_hash_magento_sales_payment_transaction] (
    [stage_hash_magento_sales_payment_transaction_id] BIGINT        IDENTITY (1, 1) NOT NULL,
    [bk_hash]                                         CHAR (32)     NOT NULL,
    [transaction_id]                                  INT           NULL,
    [parent_id]                                       INT           NULL,
    [order_id]                                        INT           NULL,
    [payment_id]                                      INT           NULL,
    [txn_id]                                          VARCHAR (100) NULL,
    [parent_txn_id]                                   VARCHAR (100) NULL,
    [txn_type]                                        VARCHAR (15)  NULL,
    [is_closed]                                       INT           NULL,
    [created_at]                                      DATETIME      NULL,
    [dv_load_date_time]                               DATETIME      NOT NULL,
    [dv_batch_id]                                     BIGINT        NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

