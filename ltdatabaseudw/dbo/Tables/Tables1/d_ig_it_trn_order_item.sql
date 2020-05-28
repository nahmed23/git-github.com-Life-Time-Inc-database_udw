CREATE TABLE [dbo].[d_ig_it_trn_order_item] (
    [d_ig_it_trn_order_item_id]              BIGINT          IDENTITY (1, 1) NOT NULL,
    [bk_hash]                                CHAR (32)       NOT NULL,
    [fact_cafe_sales_transaction_item_key]   CHAR (32)       NULL,
    [check_seq]                              SMALLINT        NULL,
    [order_hdr_id]                           INT             NULL,
    [d_ig_it_trn_order_header_bk_hash]       CHAR (32)       NULL,
    [dim_cafe_discount_coupon_key]           CHAR (32)       NULL,
    [dim_cafe_product_key]                   CHAR (32)       NULL,
    [item_discount_amount]                   DECIMAL (26, 6) NULL,
    [item_quantity]                          INT             NULL,
    [item_refund_flag]                       CHAR (1)        NULL,
    [item_sales_amount_gross]                DECIMAL (26, 6) NULL,
    [item_sales_dollar_amount_excluding_tax] DECIMAL (26, 6) NULL,
    [item_tax_amount]                        DECIMAL (26, 6) NULL,
    [item_voided_flag]                       CHAR (1)        NULL,
    [p_ig_it_trn_order_item_id]              BIGINT          NOT NULL,
    [deleted_flag]                           INT             NULL,
    [dv_load_date_time]                      DATETIME        NULL,
    [dv_load_end_date_time]                  DATETIME        NULL,
    [dv_batch_id]                            BIGINT          NOT NULL,
    [dv_inserted_date_time]                  DATETIME        NOT NULL,
    [dv_insert_user]                         VARCHAR (50)    NOT NULL,
    [dv_updated_date_time]                   DATETIME        NULL,
    [dv_update_user]                         VARCHAR (50)    NULL
)
WITH (CLUSTERED COLUMNSTORE INDEX, DISTRIBUTION = HASH([bk_hash]));

