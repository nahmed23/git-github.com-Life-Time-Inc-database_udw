﻿CREATE TABLE [dbo].[stage_hash_magento_customer_group] (
    [stage_hash_magento_customer_group_id] BIGINT       IDENTITY (1, 1) NOT NULL,
    [bk_hash]                              CHAR (32)    NOT NULL,
    [customer_group_id]                    INT          NULL,
    [customer_group_code]                  VARCHAR (32) NULL,
    [tax_class_id]                         INT          NULL,
    [dummy_modified_date_time]             DATETIME     NULL,
    [dv_load_date_time]                    DATETIME     NOT NULL,
    [dv_batch_id]                          BIGINT       NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

