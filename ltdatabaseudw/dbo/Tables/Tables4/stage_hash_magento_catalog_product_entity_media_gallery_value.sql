﻿CREATE TABLE [dbo].[stage_hash_magento_catalog_product_entity_media_gallery_value] (
    [stage_hash_magento_catalog_product_entity_media_gallery_value_id] BIGINT        IDENTITY (1, 1) NOT NULL,
    [bk_hash]                                                          CHAR (32)     NOT NULL,
    [value_id]                                                         INT           NULL,
    [store_id]                                                         INT           NULL,
    [row_id]                                                           INT           NULL,
    [label]                                                            VARCHAR (255) NULL,
    [position]                                                         INT           NULL,
    [disabled]                                                         INT           NULL,
    [record_id]                                                        INT           NULL,
    [dummy_modified_date_time]                                         DATETIME      NULL,
    [dv_load_date_time]                                                DATETIME      NOT NULL,
    [dv_batch_id]                                                      BIGINT        NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

