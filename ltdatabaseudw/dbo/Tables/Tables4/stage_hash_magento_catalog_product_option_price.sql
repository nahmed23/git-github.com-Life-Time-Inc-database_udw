﻿CREATE TABLE [dbo].[stage_hash_magento_catalog_product_option_price] (
    [stage_hash_magento_catalog_product_option_price_id] BIGINT          IDENTITY (1, 1) NOT NULL,
    [bk_hash]                                            CHAR (32)       NOT NULL,
    [option_price_id]                                    INT             NULL,
    [option_id]                                          INT             NULL,
    [store_id]                                           INT             NULL,
    [price]                                              DECIMAL (12, 4) NULL,
    [price_type]                                         VARCHAR (7)     NULL,
    [dummy_modified_date_time]                           DATETIME        NULL,
    [dv_load_date_time]                                  DATETIME        NOT NULL,
    [dv_batch_id]                                        BIGINT          NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

