﻿CREATE TABLE [dbo].[stage_magento_catalog_product_bundle_option_value] (
    [stage_magento_catalog_product_bundle_option_value_id] BIGINT         NOT NULL,
    [value_id]                                             INT            NULL,
    [option_id]                                            INT            NULL,
    [parent_product_id]                                    INT            NULL,
    [store_id]                                             INT            NULL,
    [title]                                                NVARCHAR (255) NULL,
    [dummy_modified_date_time]                             DATETIME       NULL,
    [dv_batch_id]                                          BIGINT         NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

