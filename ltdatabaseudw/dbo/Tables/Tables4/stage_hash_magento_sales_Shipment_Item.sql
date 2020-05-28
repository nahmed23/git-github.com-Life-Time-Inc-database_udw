CREATE TABLE [dbo].[stage_hash_magento_sales_Shipment_Item] (
    [stage_hash_magento_sales_Shipment_Item_id] BIGINT          IDENTITY (1, 1) NOT NULL,
    [bk_hash]                                   CHAR (32)       NOT NULL,
    [entity_id]                                 INT             NULL,
    [parent_id]                                 INT             NULL,
    [row_total]                                 DECIMAL (12, 4) NULL,
    [price]                                     DECIMAL (12, 4) NULL,
    [weight]                                    DECIMAL (12, 4) NULL,
    [qty]                                       DECIMAL (12, 4) NULL,
    [product_id]                                INT             NULL,
    [order_item_id]                             INT             NULL,
    [additional_data]                           VARCHAR (8000)  NULL,
    [description]                               VARCHAR (8000)  NULL,
    [name]                                      VARCHAR (255)   NULL,
    [sku]                                       VARCHAR (255)   NULL,
    [dummy_modified_date_time]                  DATETIME        NULL,
    [dv_load_date_time]                         DATETIME        NOT NULL,
    [dv_batch_id]                               BIGINT          NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

