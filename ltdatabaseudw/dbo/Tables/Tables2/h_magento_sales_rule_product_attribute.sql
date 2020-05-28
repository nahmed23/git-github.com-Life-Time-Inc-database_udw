﻿CREATE TABLE [dbo].[h_magento_sales_rule_product_attribute] (
    [h_magento_sales_rule_product_attribute_id] BIGINT       IDENTITY (1, 1) NOT NULL,
    [bk_hash]                                   CHAR (32)    NOT NULL,
    [row_id]                                    INT          NULL,
    [website_id]                                INT          NULL,
    [customer_group_id]                         INT          NULL,
    [attribute_id]                              INT          NULL,
    [dv_load_date_time]                         DATETIME     NOT NULL,
    [dv_r_load_source_id]                       BIGINT       NOT NULL,
    [dv_inserted_date_time]                     DATETIME     NOT NULL,
    [dv_insert_user]                            VARCHAR (50) NOT NULL,
    [dv_updated_date_time]                      DATETIME     NULL,
    [dv_update_user]                            VARCHAR (50) NULL,
    [dv_deleted]                                BIT          DEFAULT ((0)) NOT NULL,
    [dv_batch_id]                               BIGINT       NOT NULL
)
WITH (CLUSTERED INDEX([bk_hash]), DISTRIBUTION = HASH([bk_hash]));

