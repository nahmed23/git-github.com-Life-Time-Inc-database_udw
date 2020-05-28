﻿CREATE TABLE [dbo].[stage_magento_eav_attribute_option] (
    [stage_magento_eav_attribute_option_id] BIGINT   NOT NULL,
    [option_id]                             INT      NULL,
    [attribute_id]                          INT      NULL,
    [sort_order]                            INT      NULL,
    [dummy_modified_date_time]              DATETIME NULL,
    [dv_batch_id]                           BIGINT   NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

