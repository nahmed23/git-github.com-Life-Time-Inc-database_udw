CREATE TABLE [dbo].[d_lt_bucks_cart_details] (
    [d_lt_bucks_cart_details_id]       BIGINT       IDENTITY (1, 1) NOT NULL,
    [bk_hash]                          CHAR (32)    NOT NULL,
    [fact_lt_bucks_cart_details_key]   CHAR (32)    NULL,
    [cdetail_id]                       INT          NULL,
    [cart_id]                          INT          NULL,
    [delivery_date_time]               DATETIME     NULL,
    [dim_club_key]                     CHAR (32)    NULL,
    [dim_lt_bucks_product_options_key] CHAR (32)    NULL,
    [fact_lt_bucks_shopping_cart_key]  CHAR (32)    NULL,
    [fact_mms_package_key]             CHAR (32)    NULL,
    [fact_mms_sales_transaction_key]   CHAR (32)    NULL,
    [transaction_expiration_date_time] DATETIME     NULL,
    [p_lt_bucks_cart_details_id]       BIGINT       NOT NULL,
    [dv_load_date_time]                DATETIME     NULL,
    [dv_load_end_date_time]            DATETIME     NULL,
    [dv_batch_id]                      BIGINT       NOT NULL,
    [dv_inserted_date_time]            DATETIME     NOT NULL,
    [dv_insert_user]                   VARCHAR (50) NOT NULL,
    [dv_updated_date_time]             DATETIME     NULL,
    [dv_update_user]                   VARCHAR (50) NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));


GO
CREATE NONCLUSTERED INDEX [ix_dv_batch_id]
    ON [dbo].[d_lt_bucks_cart_details]([dv_batch_id] ASC);

