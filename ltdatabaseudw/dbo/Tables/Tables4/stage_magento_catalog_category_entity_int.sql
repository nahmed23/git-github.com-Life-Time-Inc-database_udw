﻿CREATE TABLE [dbo].[stage_magento_catalog_category_entity_int] (
    [stage_magento_catalog_category_entity_int_id] BIGINT   NOT NULL,
    [value_id]                                     INT      NULL,
    [attribute_id]                                 INT      NULL,
    [store_id]                                     INT      NULL,
    [row_id]                                       INT      NULL,
    [value]                                        INT      NULL,
    [dummy_modified_date_time]                     DATETIME NULL,
    [dv_batch_id]                                  BIGINT   NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

