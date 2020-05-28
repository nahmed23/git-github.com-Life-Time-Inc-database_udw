CREATE TABLE [dbo].[stage_hash_magento_cataloginventory_stock_status] (
    [stage_hash_magento_cataloginventory_stock_status_id] BIGINT          IDENTITY (1, 1) NOT NULL,
    [bk_hash]                                             CHAR (32)       NOT NULL,
    [product_id]                                          INT             NULL,
    [website_id]                                          INT             NULL,
    [stock_id]                                            INT             NULL,
    [qty]                                                 DECIMAL (12, 4) NULL,
    [stock_status]                                        INT             NULL,
    [dummy_modified_date_time]                            DATETIME        NULL,
    [dv_load_date_time]                                   DATETIME        NOT NULL,
    [dv_batch_id]                                         BIGINT          NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

