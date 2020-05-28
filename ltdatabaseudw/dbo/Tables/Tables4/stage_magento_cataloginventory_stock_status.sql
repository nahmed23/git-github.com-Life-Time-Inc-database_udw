CREATE TABLE [dbo].[stage_magento_cataloginventory_stock_status] (
    [stage_magento_cataloginventory_stock_status_id] BIGINT          NOT NULL,
    [product_id]                                     INT             NULL,
    [website_id]                                     INT             NULL,
    [stock_id]                                       INT             NULL,
    [qty]                                            DECIMAL (12, 4) NULL,
    [stock_status]                                   INT             NULL,
    [dummy_modified_date_time]                       DATETIME        NULL,
    [dv_batch_id]                                    BIGINT          NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

