﻿CREATE TABLE [dbo].[stage_hash_magento_catalog_product_option] (
    [stage_hash_magento_catalog_product_option_id] BIGINT       IDENTITY (1, 1) NOT NULL,
    [bk_hash]                                      CHAR (32)    NOT NULL,
    [option_id]                                    INT          NULL,
    [product_id]                                   INT          NULL,
    [type]                                         VARCHAR (50) NULL,
    [is_require]                                   INT          NULL,
    [sku]                                          VARCHAR (64) NULL,
    [max_characters]                               INT          NULL,
    [file_extension]                               VARCHAR (50) NULL,
    [image_size_x]                                 INT          NULL,
    [image_size_y]                                 INT          NULL,
    [sort_order]                                   INT          NULL,
    [dummy_modified_date_time]                     DATETIME     NULL,
    [dv_load_date_time]                            DATETIME     NOT NULL,
    [dv_batch_id]                                  BIGINT       NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

