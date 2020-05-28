CREATE TABLE [dbo].[tmp_tran_item] (
    [bk_hash]                             CHAR (32)       NOT NULL,
    [fact_mms_sales_transaction_item_key] CHAR (32)       NULL,
    [club_id]                             INT             NULL,
    [dim_club_key]                        CHAR (32)       NULL,
    [dim_mms_product_key]                 CHAR (32)       NULL,
    [fact_mms_sales_transaction_key]      CHAR (32)       NULL,
    [sales_amount_gross]                  DECIMAL (26, 6) NULL,
    [sales_discount_dollar_amount]        DECIMAL (26, 6) NULL,
    [sales_dollar_amount]                 DECIMAL (26, 6) NULL,
    [sales_quantity]                      INT             NULL,
    [sales_tax_amount]                    DECIMAL (26, 6) NULL,
    [sold_not_serviced_flag]              CHAR (1)        NULL,
    [tran_item_id]                        INT             NULL,
    [dv_batch_id]                         BIGINT          NOT NULL,
    [dv_load_date_time]                   DATETIME        NULL
)
WITH (HEAP, DISTRIBUTION = HASH([fact_mms_sales_transaction_key]));

