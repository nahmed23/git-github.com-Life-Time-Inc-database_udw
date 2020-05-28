CREATE TABLE [dbo].[stage_magento_cataloginventory_stock] (
    [stage_magento_cataloginventory_stock_id] BIGINT        NOT NULL,
    [stock_id]                                INT           NULL,
    [website_id]                              INT           NULL,
    [stock_name]                              VARCHAR (255) NULL,
    [dummy_modified_date_time]                DATETIME      NULL,
    [dv_batch_id]                             BIGINT        NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

