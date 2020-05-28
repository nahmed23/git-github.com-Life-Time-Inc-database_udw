CREATE TABLE [dbo].[s_hybris_all_products] (
    [s_hybris_all_products_id] BIGINT          IDENTITY (1, 1) NOT NULL,
    [bk_hash]                  CHAR (32)       NOT NULL,
    [created_ts]               DATETIME        NULL,
    [creation_time]            DATETIME        NULL,
    [code]                     NVARCHAR (258)  NULL,
    [name]                     NVARCHAR (1545) NULL,
    [online_datetime]          DATETIME        NULL,
    [offline_datetime]         DATETIME        NULL,
    [summary]                  NVARCHAR (4000) NULL,
    [description]              NVARCHAR (4000) NULL,
    [weight]                   DECIMAL (26, 6) NULL,
    [modified_time]            DATETIME        NULL,
    [caption]                  NVARCHAR (255)  NULL,
    [ean]                      NVARCHAR (255)  NULL,
    [product_cost]             DECIMAL (26, 6) NULL,
    [product_height]           DECIMAL (26, 6) NULL,
    [product_width]            DECIMAL (26, 6) NULL,
    [product_length]           DECIMAL (26, 6) NULL,
    [auto_ship_flag]           TINYINT         NULL,
    [electronic_shipping_flag] TINYINT         NULL,
    [fulfillment_partner]      NVARCHAR (255)  NULL,
    [ltf_only_product]         TINYINT         NULL,
    [ltf_offer_flag]           TINYINT         NULL,
    [offer_external_link_flag] TINYINT         NULL,
    [e_gift_card_flag]         TINYINT         NULL,
    [offer_link]               NVARCHAR (1000) NULL,
    [catalog_name]             NVARCHAR (200)  NULL,
    [catalog_version_name]     NVARCHAR (255)  NULL,
    [product_category]         NVARCHAR (1545) NULL,
    [product_sub_category]     NVARCHAR (1545) NULL,
    [product_type]             VARCHAR (18)    NULL,
    [product_stock_level]      INT             NULL,
    [product_stock_status]     NVARCHAR (255)  NULL,
    [lt_bucks_earned]          DECIMAL (26, 6) NULL,
    [accept_lt_bucks_flag]     TINYINT         NULL,
    [dv_load_date_time]        DATETIME        NOT NULL,
    [dv_r_load_source_id]      BIGINT          NOT NULL,
    [dv_inserted_date_time]    DATETIME        NOT NULL,
    [dv_insert_user]           VARCHAR (50)    NOT NULL,
    [dv_updated_date_time]     DATETIME        NULL,
    [dv_update_user]           VARCHAR (50)    NULL,
    [dv_hash]                  CHAR (32)       NOT NULL,
    [dv_batch_id]              BIGINT          NOT NULL
)
WITH (CLUSTERED INDEX([bk_hash]), DISTRIBUTION = HASH([bk_hash]));


GO
CREATE NONCLUSTERED INDEX [ix_dv_batch_id]
    ON [dbo].[s_hybris_all_products]([dv_batch_id] ASC);

