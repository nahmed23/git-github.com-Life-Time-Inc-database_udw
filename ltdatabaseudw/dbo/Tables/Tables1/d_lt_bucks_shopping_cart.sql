CREATE TABLE [dbo].[d_lt_bucks_shopping_cart] (
    [d_lt_bucks_shopping_cart_id]     BIGINT          IDENTITY (1, 1) NOT NULL,
    [bk_hash]                         CHAR (32)       NOT NULL,
    [fact_lt_bucks_shopping_cart_key] CHAR (32)       NULL,
    [cart_id]                         INT             NULL,
    [bucks_amount]                    DECIMAL (26, 6) NULL,
    [dim_lt_bucks_products_key]       CHAR (32)       NULL,
    [name]                            NVARCHAR (150)  NULL,
    [point_amount]                    DECIMAL (26, 6) NULL,
    [product_sku]                     NVARCHAR (15)   NULL,
    [quantity]                        INT             NULL,
    [status]                          INT             NULL,
    [transaction_date_time]           DATETIME        NULL,
    [p_lt_bucks_shopping_cart_id]     BIGINT          NOT NULL,
    [dv_load_date_time]               DATETIME        NULL,
    [dv_load_end_date_time]           DATETIME        NULL,
    [dv_batch_id]                     BIGINT          NOT NULL,
    [dv_inserted_date_time]           DATETIME        NOT NULL,
    [dv_insert_user]                  VARCHAR (50)    NOT NULL,
    [dv_updated_date_time]            DATETIME        NULL,
    [dv_update_user]                  VARCHAR (50)    NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));


GO
CREATE NONCLUSTERED INDEX [ix_dv_batch_id]
    ON [dbo].[d_lt_bucks_shopping_cart]([dv_batch_id] ASC);

