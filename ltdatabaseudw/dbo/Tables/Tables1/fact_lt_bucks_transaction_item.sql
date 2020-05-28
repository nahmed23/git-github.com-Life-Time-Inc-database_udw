CREATE TABLE [dbo].[fact_lt_bucks_transaction_item] (
    [fact_lt_bucks_transaction_item_id]  BIGINT          IDENTITY (1, 1) NOT NULL,
    [fact_lt_bucks_transaction_item_key] CHAR (32)       NULL,
    [cart_id]                            INT             NULL,
    [bucks_amount]                       DECIMAL (26, 6) NULL,
    [cart_detail_id]                     INT             NULL,
    [cart_name]                          VARCHAR (150)   NULL,
    [delivery_date_time]                 DATETIME        NULL,
    [dim_club_key]                       CHAR (32)       NULL,
    [dim_lt_bucks_product_option_key]    CHAR (32)       NULL,
    [fact_mms_package_key]               CHAR (32)       NULL,
    [fact_mms_sales_transaction_key]     CHAR (32)       NULL,
    [product_sku]                        VARCHAR (15)    NULL,
    [quantity]                           INT             NULL,
    [session_id]                         INT             NULL,
    [transaction_date_time]              DATETIME        NULL,
    [transaction_expiration_date_time]   DATETIME        NULL,
    [p_lt_bucks_cart_details_id]         BIGINT          NULL,
    [p_lt_bucks_shopping_cart_id]        BIGINT          NULL,
    [dv_load_date_time]                  DATETIME        NULL,
    [dv_load_end_date_time]              DATETIME        NULL,
    [dv_batch_id]                        BIGINT          NULL,
    [dv_inserted_date_time]              DATETIME        NOT NULL,
    [dv_insert_user]                     VARCHAR (50)    NOT NULL,
    [dv_updated_date_time]               DATETIME        NULL,
    [dv_update_user]                     VARCHAR (50)    NULL
)
WITH (HEAP, DISTRIBUTION = HASH([fact_lt_bucks_transaction_item_key]));

