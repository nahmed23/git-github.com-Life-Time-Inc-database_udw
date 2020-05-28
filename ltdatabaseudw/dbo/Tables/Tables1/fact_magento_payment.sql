CREATE TABLE [dbo].[fact_magento_payment] (
    [fact_magento_payment_id]              BIGINT          IDENTITY (1, 1) NOT NULL,
    [base_amount_authorized]               DECIMAL (12, 4) NULL,
    [base_amount_ordered]                  DECIMAL (12, 4) NULL,
    [base_amount_paid]                     DECIMAL (12, 4) NULL,
    [batch_number]                         INT             NULL,
    [cc_last_4]                            INT             NULL,
    [cc_type]                              CHAR (8)        NULL,
    [created_dim_date_key]                 CHAR (8)        NULL,
    [created_dim_time_key]                 CHAR (8)        NULL,
    [credit_tran_id]                       INT             NULL,
    [fact_magento_payment_key]             CHAR (32)       NULL,
    [fact_magento_sales_order_key]         CHAR (32)       NULL,
    [fact_magento_sales_order_payment_key] CHAR (32)       NULL,
    [sales_order_payment_id]               INT             NULL,
    [transaction_id]                       INT             NULL,
    [txn_type]                             VARCHAR (50)    NULL,
    [dv_load_date_time]                    DATETIME        NULL,
    [dv_load_end_date_time]                DATETIME        NULL,
    [dv_batch_id]                          BIGINT          NOT NULL,
    [dv_inserted_date_time]                DATETIME        NOT NULL,
    [dv_insert_user]                       VARCHAR (50)    NOT NULL,
    [dv_updated_date_time]                 DATETIME        NULL,
    [dv_update_user]                       VARCHAR (50)    NULL
)
WITH (CLUSTERED COLUMNSTORE INDEX, DISTRIBUTION = HASH([fact_magento_payment_key]));

