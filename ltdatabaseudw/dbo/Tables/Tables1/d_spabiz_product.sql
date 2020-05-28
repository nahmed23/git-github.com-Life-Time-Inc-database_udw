CREATE TABLE [dbo].[d_spabiz_product] (
    [d_spabiz_product_id]              BIGINT          IDENTITY (1, 1) NOT NULL,
    [bk_hash]                          CHAR (32)       NOT NULL,
    [dim_spabiz_product_key]           CHAR (32)       NULL,
    [product_id]                       BIGINT          NULL,
    [store_number]                     BIGINT          NULL,
    [avg_cost]                         DECIMAL (26, 6) NULL,
    [cost]                             DECIMAL (26, 6) NULL,
    [cost2]                            DECIMAL (26, 6) NULL,
    [cost2_quantity]                   INT             NULL,
    [created_date_time]                DATETIME        NULL,
    [current_quantity]                 INT             NULL,
    [deleted_date_time]                DATETIME        NULL,
    [deleted_flag]                     CHAR (1)        NULL,
    [dim_spabiz_category_key]          CHAR (32)       NULL,
    [dim_spabiz_manufacturer_key]      CHAR (32)       NULL,
    [dim_spabiz_staff_key]             CHAR (32)       NULL,
    [dim_spabiz_store_key]             CHAR (32)       NULL,
    [dim_spabiz_sub_category_key]      CHAR (32)       NULL,
    [dim_spabiz_vendor_key]            CHAR (32)       NULL,
    [economic_order_quantity]          INT             NULL,
    [edit_date_time]                   DATETIME        NULL,
    [gl_account]                       VARCHAR (45)    NULL,
    [label_name]                       VARCHAR (150)   NULL,
    [last_count_date_time]             DATETIME        NULL,
    [last_purchased_date_time]         DATETIME        NULL,
    [last_sold_date_time]              DATETIME        NULL,
    [location]                         VARCHAR (150)   NULL,
    [manufacturer_code]                VARCHAR (150)   NULL,
    [maximum_inventory_count]          INT             NULL,
    [minimum_inventory_count]          INT             NULL,
    [on_order]                         INT             NULL,
    [print_label_flag]                 CHAR (1)        NULL,
    [print_on_ticket]                  VARCHAR (765)   NULL,
    [product_name]                     VARCHAR (195)   NULL,
    [product_type_dim_description_key] VARCHAR (50)    NULL,
    [product_type_id]                  VARCHAR (50)    NULL,
    [quick_id]                         VARCHAR (90)    NULL,
    [retail_price]                     DECIMAL (26, 6) NULL,
    [taxable_flag]                     CHAR (1)        NULL,
    [vendor_code]                      VARCHAR (150)   NULL,
    [p_spabiz_product_id]              BIGINT          NOT NULL,
    [dv_load_date_time]                DATETIME        NULL,
    [dv_load_end_date_time]            DATETIME        NULL,
    [dv_batch_id]                      BIGINT          NOT NULL,
    [dv_inserted_date_time]            DATETIME        NOT NULL,
    [dv_insert_user]                   VARCHAR (50)    NOT NULL,
    [dv_updated_date_time]             DATETIME        NULL,
    [dv_update_user]                   VARCHAR (50)    NULL
)
WITH (HEAP, DISTRIBUTION = REPLICATE);


GO
CREATE NONCLUSTERED INDEX [ix_dv_batch_id]
    ON [dbo].[d_spabiz_product]([dv_batch_id] ASC);


GO
CREATE STATISTICS [stat_dv_batch_id]
    ON [dbo].[d_spabiz_product]([dv_batch_id]);

