CREATE TABLE [dbo].[d_magento_sales_shipment_item] (
    [d_magento_sales_shipment_item_id] BIGINT          IDENTITY (1, 1) NOT NULL,
    [bk_hash]                          CHAR (32)       NOT NULL,
    [sales_shipment_item_id]           INT             NULL,
    [d_magento_sales_shipment_bk_hash] CHAR (32)       NULL,
    [dim_magento_product_key]          CHAR (32)       NULL,
    [fact_magento_order_item_key]      VARCHAR (32)    NULL,
    [fact_magento_shipment_key]        VARCHAR (32)    NULL,
    [order_item_id]                    INT             NULL,
    [row_total]                        DECIMAL (12, 4) NULL,
    [sales_shipment_item_description]  VARCHAR (8000)  NULL,
    [sales_shipment_item_name]         VARCHAR (8000)  NULL,
    [sales_shipment_item_price]        DECIMAL (12, 4) NULL,
    [sales_shipment_item_qty]          DECIMAL (12, 4) NULL,
    [sales_shipment_item_sku]          VARCHAR (255)   NULL,
    [sales_shipment_item_weight]       DECIMAL (12, 4) NULL,
    [p_magento_sales_shipment_item_id] BIGINT          NOT NULL,
    [deleted_flag]                     INT             NULL,
    [dv_load_date_time]                DATETIME        NULL,
    [dv_load_end_date_time]            DATETIME        NULL,
    [dv_batch_id]                      BIGINT          NOT NULL,
    [dv_inserted_date_time]            DATETIME        NOT NULL,
    [dv_insert_user]                   VARCHAR (50)    NOT NULL,
    [dv_updated_date_time]             DATETIME        NULL,
    [dv_update_user]                   VARCHAR (50)    NULL
)
WITH (CLUSTERED COLUMNSTORE INDEX, DISTRIBUTION = HASH([bk_hash]));

