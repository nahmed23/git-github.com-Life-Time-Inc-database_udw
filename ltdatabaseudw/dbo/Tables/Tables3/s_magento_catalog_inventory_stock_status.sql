﻿CREATE TABLE [dbo].[s_magento_catalog_inventory_stock_status] (
    [s_magento_catalog_inventory_stock_status_id] BIGINT          IDENTITY (1, 1) NOT NULL,
    [bk_hash]                                     CHAR (32)       NOT NULL,
    [product_id]                                  INT             NULL,
    [website_id]                                  INT             NULL,
    [stock_id]                                    INT             NULL,
    [qty]                                         DECIMAL (12, 4) NULL,
    [stock_status]                                INT             NULL,
    [dummy_modified_date_time]                    DATETIME        NULL,
    [dv_load_date_time]                           DATETIME        NOT NULL,
    [dv_r_load_source_id]                         BIGINT          NOT NULL,
    [dv_inserted_date_time]                       DATETIME        NOT NULL,
    [dv_insert_user]                              VARCHAR (50)    NOT NULL,
    [dv_updated_date_time]                        DATETIME        NULL,
    [dv_update_user]                              VARCHAR (50)    NULL,
    [dv_hash]                                     CHAR (32)       NOT NULL,
    [dv_deleted]                                  BIT             DEFAULT ((0)) NOT NULL,
    [dv_batch_id]                                 BIGINT          NOT NULL
)
WITH (CLUSTERED INDEX([bk_hash]), DISTRIBUTION = HASH([bk_hash]));
