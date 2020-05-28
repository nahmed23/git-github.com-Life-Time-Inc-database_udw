﻿CREATE TABLE [dbo].[l_magento_catalog_product_entity_decimal] (
    [l_magento_catalog_product_entity_decimal_id] BIGINT       IDENTITY (1, 1) NOT NULL,
    [bk_hash]                                     CHAR (32)    NOT NULL,
    [value_id]                                    INT          NULL,
    [attribute_id]                                INT          NULL,
    [store_id]                                    INT          NULL,
    [row_id]                                      INT          NULL,
    [dv_load_date_time]                           DATETIME     NOT NULL,
    [dv_r_load_source_id]                         BIGINT       NOT NULL,
    [dv_inserted_date_time]                       DATETIME     NOT NULL,
    [dv_insert_user]                              VARCHAR (50) NOT NULL,
    [dv_updated_date_time]                        DATETIME     NULL,
    [dv_update_user]                              VARCHAR (50) NULL,
    [dv_hash]                                     CHAR (32)    NOT NULL,
    [dv_deleted]                                  BIT          DEFAULT ((0)) NOT NULL,
    [dv_batch_id]                                 BIGINT       NOT NULL
)
WITH (CLUSTERED INDEX([bk_hash]), DISTRIBUTION = HASH([bk_hash]));

