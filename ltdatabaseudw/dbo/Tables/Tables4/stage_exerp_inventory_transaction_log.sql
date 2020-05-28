﻿CREATE TABLE [dbo].[stage_exerp_inventory_transaction_log] (
    [stage_exerp_inventory_transaction_log_id] BIGINT          NOT NULL,
    [id]                                       INT             NULL,
    [inventory_id]                             INT             NULL,
    [inventory_name]                           VARCHAR (4000)  NULL,
    [type]                                     VARCHAR (4000)  NULL,
    [comment]                                  VARCHAR (4000)  NULL,
    [product_id]                               VARCHAR (4000)  NULL,
    [book_datetime]                            DATETIME        NULL,
    [quantity]                                 INT             NULL,
    [unit_value]                               DECIMAL (26, 6) NULL,
    [balance_quantity]                         INT             NULL,
    [balance_value]                            DECIMAL (26, 6) NULL,
    [center_id]                                INT             NULL,
    [ets]                                      BIGINT          NULL,
    [dummy_modified_date_time]                 DATETIME        NULL,
    [dv_batch_id]                              BIGINT          NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

