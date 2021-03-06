﻿CREATE TABLE [dbo].[stage_magento_catalog_product_website] (
    [stage_magento_catalog_product_website_id] BIGINT   NOT NULL,
    [product_id]                               INT      NULL,
    [website_id]                               INT      NULL,
    [dummy_modified_date_time]                 DATETIME NULL,
    [dv_batch_id]                              BIGINT   NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

