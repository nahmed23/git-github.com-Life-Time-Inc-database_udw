CREATE TABLE [dbo].[d_magento_catalog_rule_product_price] (
    [d_magento_catalog_rule_product_price_id] BIGINT          IDENTITY (1, 1) NOT NULL,
    [bk_hash]                                 CHAR (32)       NOT NULL,
    [rule_product_price_id]                   INT             NULL,
    [customer_group_id]                       INT             NULL,
    [earliest_end_date]                       DATETIME        NULL,
    [earliest_end_dim_date_key]               CHAR (8)        NULL,
    [latest_start_date]                       DATETIME        NULL,
    [latest_start_dim_date_key]               CHAR (8)        NULL,
    [product_id]                              INT             NULL,
    [rule_date]                               DATETIME        NULL,
    [rule_dim_date_key]                       CHAR (8)        NULL,
    [rule_price]                              DECIMAL (12, 4) NULL,
    [website_id]                              INT             NULL,
    [p_magento_catalog_rule_product_price_id] BIGINT          NOT NULL,
    [deleted_flag]                            INT             NULL,
    [dv_load_date_time]                       DATETIME        NULL,
    [dv_load_end_date_time]                   DATETIME        NULL,
    [dv_batch_id]                             BIGINT          NOT NULL,
    [dv_inserted_date_time]                   DATETIME        NOT NULL,
    [dv_insert_user]                          VARCHAR (50)    NOT NULL,
    [dv_updated_date_time]                    DATETIME        NULL,
    [dv_update_user]                          VARCHAR (50)    NULL
)
WITH (CLUSTERED COLUMNSTORE INDEX, DISTRIBUTION = HASH([bk_hash]));

