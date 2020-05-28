CREATE TABLE [dbo].[fact_magento_shipment_item] (
    [fact_magento_shipment_item_id]  BIGINT          IDENTITY (1, 1) NOT NULL,
    [billing_address_id]             INT             NULL,
    [dim_magento_product_key]        VARCHAR (32)    NULL,
    [fact_magento_order_item_key]    VARCHAR (32)    NULL,
    [fact_magento_order_key]         VARCHAR (32)    NULL,
    [fact_magento_shipment_item_key] VARCHAR (32)    NULL,
    [fact_magento_shipment_key]      VARCHAR (32)    NULL,
    [shipment_datetime]              DATETIME        NULL,
    [shipment_dim_date_key]          VARCHAR (8)     NULL,
    [shipment_item_id]               INT             NULL,
    [shipment_item_price]            DECIMAL (12, 2) NULL,
    [shipment_item_quantity]         INT             NULL,
    [shipment_status]                INT             NULL,
    [shipping_address_id]            INT             NULL,
    [dv_load_date_time]              DATETIME        NULL,
    [dv_load_end_date_time]          DATETIME        NULL,
    [dv_batch_id]                    BIGINT          NOT NULL,
    [dv_inserted_date_time]          DATETIME        NOT NULL,
    [dv_insert_user]                 VARCHAR (50)    NOT NULL,
    [dv_updated_date_time]           DATETIME        NULL,
    [dv_update_user]                 VARCHAR (50)    NULL
)
WITH (CLUSTERED COLUMNSTORE INDEX, DISTRIBUTION = HASH([fact_magento_shipment_item_key]));

