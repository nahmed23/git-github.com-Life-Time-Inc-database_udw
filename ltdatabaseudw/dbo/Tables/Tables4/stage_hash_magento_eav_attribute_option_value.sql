﻿CREATE TABLE [dbo].[stage_hash_magento_eav_attribute_option_value] (
    [stage_hash_magento_eav_attribute_option_value_id] BIGINT        IDENTITY (1, 1) NOT NULL,
    [bk_hash]                                          CHAR (32)     NOT NULL,
    [value_id]                                         INT           NULL,
    [option_id]                                        INT           NULL,
    [store_id]                                         INT           NULL,
    [value]                                            VARCHAR (255) NULL,
    [dummy_modified_date_time]                         DATETIME      NULL,
    [dv_load_date_time]                                DATETIME      NOT NULL,
    [dv_batch_id]                                      BIGINT        NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

