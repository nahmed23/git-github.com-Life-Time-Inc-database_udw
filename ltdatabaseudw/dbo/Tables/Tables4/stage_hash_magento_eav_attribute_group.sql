﻿CREATE TABLE [dbo].[stage_hash_magento_eav_attribute_group] (
    [stage_hash_magento_eav_attribute_group_id] BIGINT        IDENTITY (1, 1) NOT NULL,
    [bk_hash]                                   CHAR (32)     NOT NULL,
    [attribute_group_id]                        INT           NULL,
    [attribute_set_id]                          INT           NULL,
    [attribute_group_name]                      VARCHAR (255) NULL,
    [sort_order]                                INT           NULL,
    [default_id]                                INT           NULL,
    [attribute_group_code]                      VARCHAR (255) NULL,
    [tab_group_code]                            VARCHAR (255) NULL,
    [dummy_modified_date_time]                  DATETIME      NULL,
    [dv_load_date_time]                         DATETIME      NOT NULL,
    [dv_batch_id]                               BIGINT        NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

