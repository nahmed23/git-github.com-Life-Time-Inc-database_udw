CREATE TABLE [dbo].[d_magento_sales_payment_transaction] (
    [d_magento_sales_payment_transaction_id] BIGINT        IDENTITY (1, 1) NOT NULL,
    [bk_hash]                                CHAR (32)     NOT NULL,
    [transaction_id]                         INT           NULL,
    [created_at]                             DATETIME      NULL,
    [created_dim_date_key]                   CHAR (8)      NULL,
    [created_dim_time_key]                   CHAR (8)      NULL,
    [fact_magento_payment_key]               CHAR (32)     NULL,
    [fact_magento_sales_order_key]           CHAR (32)     NULL,
    [fact_magento_sales_order_payment_key]   CHAR (32)     NULL,
    [is_closed_flag]                         CHAR (1)      NULL,
    [parent_id]                              INT           NULL,
    [parent_txn_id]                          VARCHAR (100) NULL,
    [txn_id]                                 VARCHAR (100) NULL,
    [txn_type]                               VARCHAR (15)  NULL,
    [p_magento_sales_payment_transaction_id] BIGINT        NOT NULL,
    [deleted_flag]                           INT           NULL,
    [dv_load_date_time]                      DATETIME      NULL,
    [dv_load_end_date_time]                  DATETIME      NULL,
    [dv_batch_id]                            BIGINT        NOT NULL,
    [dv_inserted_date_time]                  DATETIME      NOT NULL,
    [dv_insert_user]                         VARCHAR (50)  NOT NULL,
    [dv_updated_date_time]                   DATETIME      NULL,
    [dv_update_user]                         VARCHAR (50)  NULL
)
WITH (CLUSTERED COLUMNSTORE INDEX, DISTRIBUTION = HASH([bk_hash]));

