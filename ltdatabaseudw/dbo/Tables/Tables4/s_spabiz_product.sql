CREATE TABLE [dbo].[s_spabiz_product] (
    [s_spabiz_product_id]   BIGINT          IDENTITY (1, 1) NOT NULL,
    [bk_hash]               CHAR (32)       NOT NULL,
    [product_id]            DECIMAL (26, 6) NULL,
    [counter_id]            DECIMAL (26, 6) NULL,
    [edit_time]             DATETIME        NULL,
    [product_delete]        DECIMAL (26, 6) NULL,
    [delete_date]           DATETIME        NULL,
    [name]                  VARCHAR (195)   NULL,
    [quick_id]              VARCHAR (90)    NULL,
    [vendor_code]           VARCHAR (150)   NULL,
    [man_code]              VARCHAR (150)   NULL,
    [cost]                  DECIMAL (26, 6) NULL,
    [current_cost]          DECIMAL (26, 6) NULL,
    [current_layer]         DECIMAL (26, 6) NULL,
    [retail_price]          DECIMAL (26, 6) NULL,
    [taxable]               DECIMAL (26, 6) NULL,
    [type]                  DECIMAL (26, 6) NULL,
    [purchase_tax]          DECIMAL (26, 6) NULL,
    [date_created]          DATETIME        NULL,
    [width]                 DECIMAL (26, 6) NULL,
    [height]                DECIMAL (26, 6) NULL,
    [depth]                 DECIMAL (26, 6) NULL,
    [weight]                DECIMAL (26, 6) NULL,
    [order_freq]            DECIMAL (26, 6) NULL,
    [case_qty]              DECIMAL (26, 6) NULL,
    [location]              VARCHAR (150)   NULL,
    [eoq]                   DECIMAL (26, 6) NULL,
    [seasonal]              DECIMAL (26, 6) NULL,
    [min]                   DECIMAL (26, 6) NULL,
    [max]                   DECIMAL (26, 6) NULL,
    [label_name]            VARCHAR (150)   NULL,
    [print_labels]          DECIMAL (26, 6) NULL,
    [cost2]                 DECIMAL (26, 6) NULL,
    [cost2_qty]             DECIMAL (26, 6) NULL,
    [cost3]                 DECIMAL (26, 6) NULL,
    [cost3_qty]             DECIMAL (26, 6) NULL,
    [current_qty]           DECIMAL (26, 6) NULL,
    [print_on_ticket]       VARCHAR (765)   NULL,
    [status]                VARCHAR (150)   NULL,
    [labels]                DECIMAL (26, 6) NULL,
    [on_order]              DECIMAL (26, 6) NULL,
    [note]                  VARCHAR (3000)  NULL,
    [last_sold]             DATETIME        NULL,
    [last_count]            DATETIME        NULL,
    [store_rank]            DECIMAL (26, 6) NULL,
    [stock_level]           DECIMAL (26, 6) NULL,
    [quarter_sales]         DECIMAL (26, 6) NULL,
    [min_days]              DECIMAL (26, 6) NULL,
    [max_days]              DECIMAL (26, 6) NULL,
    [last_purchase]         DATETIME        NULL,
    [avg_cost]              DECIMAL (26, 6) NULL,
    [active]                DECIMAL (26, 6) NULL,
    [store_number]          DECIMAL (26, 6) NULL,
    [guid_link]             VARCHAR (300)   NULL,
    [guid_source_id]        VARCHAR (300)   NULL,
    [product_level]         DECIMAL (26, 6) NULL,
    [new_id]                DECIMAL (26, 6) NULL,
    [dv_load_date_time]     DATETIME        NOT NULL,
    [dv_batch_id]           BIGINT          NOT NULL,
    [dv_r_load_source_id]   BIGINT          NOT NULL,
    [dv_inserted_date_time] DATETIME        NOT NULL,
    [dv_insert_user]        VARCHAR (50)    NOT NULL,
    [dv_updated_date_time]  DATETIME        NULL,
    [dv_update_user]        VARCHAR (50)    NULL,
    [dv_hash]               CHAR (32)       NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));


GO
CREATE CLUSTERED INDEX [ci_s_spabiz_product]
    ON [dbo].[s_spabiz_product]([bk_hash] ASC, [s_spabiz_product_id] ASC);


GO
CREATE NONCLUSTERED INDEX [ix_dv_batch_id]
    ON [dbo].[s_spabiz_product]([dv_batch_id] ASC);

