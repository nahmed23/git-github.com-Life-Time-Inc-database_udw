CREATE TABLE [dbo].[stage_lt_bucks_ShoppingCart] (
    [stage_lt_bucks_ShoppingCart_id] BIGINT          NOT NULL,
    [cart_id]                        INT             NULL,
    [cart_session]                   INT             NULL,
    [cart_product]                   INT             NULL,
    [cart_qty]                       INT             NULL,
    [cart_status]                    INT             NULL,
    [cart_color]                     INT             NULL,
    [cart_size]                      INT             NULL,
    [cart_ext_data1]                 DECIMAL (26, 6) NULL,
    [cart_logo]                      INT             NULL,
    [cart_amount]                    DECIMAL (26, 6) NULL,
    [cart_point_amount]              DECIMAL (26, 6) NULL,
    [cart_sku]                       NVARCHAR (15)   NULL,
    [cart_sku2]                      NVARCHAR (10)   NULL,
    [cart_name]                      NVARCHAR (150)  NULL,
    [cart_locked]                    BIT             NULL,
    [cart_timestamp]                 DATETIME2 (7)   NULL,
    [cart_coupon_amount]             DECIMAL (26, 6) NULL,
    [LastModifiedTimestamp]          DATETIME        NULL,
    [dv_batch_id]                    BIGINT          NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

