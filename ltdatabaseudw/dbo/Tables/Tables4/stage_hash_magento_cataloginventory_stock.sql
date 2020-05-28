CREATE TABLE [dbo].[stage_hash_magento_cataloginventory_stock] (
    [stage_hash_magento_cataloginventory_stock_id] BIGINT        IDENTITY (1, 1) NOT NULL,
    [bk_hash]                                      CHAR (32)     NOT NULL,
    [stock_id]                                     INT           NULL,
    [website_id]                                   INT           NULL,
    [stock_name]                                   VARCHAR (255) NULL,
    [dummy_modified_date_time]                     DATETIME      NULL,
    [dv_load_date_time]                            DATETIME      NOT NULL,
    [dv_batch_id]                                  BIGINT        NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

