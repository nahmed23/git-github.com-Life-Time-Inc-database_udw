﻿CREATE TABLE [dbo].[s_magento_catalog_rule_product_price] (
    [s_magento_catalog_rule_product_price_id] BIGINT          IDENTITY (1, 1) NOT NULL,
    [bk_hash]                                 CHAR (32)       NOT NULL,
    [rule_product_price_id]                   INT             NULL,
    [rule_date]                               DATETIME        NULL,
    [rule_price]                              DECIMAL (12, 4) NULL,
    [latest_start_date]                       DATETIME        NULL,
    [earliest_end_date]                       DATETIME        NULL,
    [dv_load_date_time]                       DATETIME        NOT NULL,
    [dv_r_load_source_id]                     BIGINT          NOT NULL,
    [dv_inserted_date_time]                   DATETIME        NOT NULL,
    [dv_insert_user]                          VARCHAR (50)    NOT NULL,
    [dv_updated_date_time]                    DATETIME        NULL,
    [dv_update_user]                          VARCHAR (50)    NULL,
    [dv_hash]                                 CHAR (32)       NOT NULL,
    [dv_deleted]                              BIT             DEFAULT ((0)) NOT NULL,
    [dv_batch_id]                             BIGINT          NOT NULL
)
WITH (CLUSTERED INDEX([bk_hash]), DISTRIBUTION = HASH([bk_hash]));

