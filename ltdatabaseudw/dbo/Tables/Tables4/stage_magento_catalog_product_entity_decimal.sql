﻿CREATE TABLE [dbo].[stage_magento_catalog_product_entity_decimal] (
    [stage_magento_catalog_product_entity_decimal_id] BIGINT          NOT NULL,
    [value_id]                                        INT             NULL,
    [attribute_id]                                    INT             NULL,
    [store_id]                                        INT             NULL,
    [row_id]                                          INT             NULL,
    [value]                                           DECIMAL (12, 4) NULL,
    [dummy_modified_date_time]                        DATETIME        NULL,
    [dv_batch_id]                                     BIGINT          NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

