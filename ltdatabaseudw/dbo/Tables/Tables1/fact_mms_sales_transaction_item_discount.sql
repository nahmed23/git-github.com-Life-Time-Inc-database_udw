CREATE TABLE [dbo].[fact_mms_sales_transaction_item_discount] (
    [fact_mms_sales_transaction_item_discount_id]          BIGINT          IDENTITY (1, 1) NOT NULL,
    [dim_mms_pricing_discount_key]                         CHAR (32)       NULL,
    [discount_amount]                                      DECIMAL (26, 6) NULL,
    [fact_mms_sales_transaction_edw_inserted_dim_date_key] CHAR (32)       NULL,
    [fact_mms_sales_transaction_item_discount_key]         CHAR (32)       NULL,
    [original_currency_code]                               CHAR (3)        NULL,
    [tran_item_discount_id]                                INT             NULL,
    [tran_item_id]                                         INT             NULL,
    [usd_dim_plan_exchange_rate_key]                       CHAR (32)       NULL,
    [usd_monthly_average_dim_exchange_rate_key]            CHAR (32)       NULL,
    [dv_load_date_time]                                    DATETIME        NULL,
    [dv_load_end_date_time]                                DATETIME        NULL,
    [dv_batch_id]                                          BIGINT          NOT NULL,
    [dv_inserted_date_time]                                DATETIME        NOT NULL,
    [dv_insert_user]                                       VARCHAR (50)    NOT NULL,
    [dv_updated_date_time]                                 DATETIME        NULL,
    [dv_update_user]                                       VARCHAR (50)    NULL
)
WITH (HEAP, DISTRIBUTION = HASH([fact_mms_sales_transaction_item_discount_key]));

