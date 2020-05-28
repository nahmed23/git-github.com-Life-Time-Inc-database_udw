CREATE TABLE [dbo].[d_magento_catalog_rule_product] (
    [d_magento_catalog_rule_product_id]      BIGINT          IDENTITY (1, 1) NOT NULL,
    [bk_hash]                                CHAR (32)       NOT NULL,
    [rule_product_id]                        INT             NULL,
    [action_amount]                          DECIMAL (12, 4) NULL,
    [action_operator]                        VARCHAR (10)    NULL,
    [action_stop]                            INT             NULL,
    [catalog_rule_product_from_dim_time_key] CHAR (8)        NULL,
    [catalog_rule_product_to_dim_time_key]   CHAR (8)        NULL,
    [customer_group_id]                      INT             NULL,
    [product_id]                             INT             NULL,
    [rule_id]                                INT             NULL,
    [sort_order]                             INT             NULL,
    [website_id]                             INT             NULL,
    [p_magento_catalog_rule_product_id]      BIGINT          NOT NULL,
    [deleted_flag]                           INT             NULL,
    [dv_load_date_time]                      DATETIME        NULL,
    [dv_load_end_date_time]                  DATETIME        NULL,
    [dv_batch_id]                            BIGINT          NOT NULL,
    [dv_inserted_date_time]                  DATETIME        NOT NULL,
    [dv_insert_user]                         VARCHAR (50)    NOT NULL,
    [dv_updated_date_time]                   DATETIME        NULL,
    [dv_update_user]                         VARCHAR (50)    NULL
)
WITH (CLUSTERED COLUMNSTORE INDEX, DISTRIBUTION = HASH([bk_hash]));

