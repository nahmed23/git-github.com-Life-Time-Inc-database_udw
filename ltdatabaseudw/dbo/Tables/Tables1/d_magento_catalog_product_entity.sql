CREATE TABLE [dbo].[d_magento_catalog_product_entity] (
    [d_magento_catalog_product_entity_id] BIGINT       IDENTITY (1, 1) NOT NULL,
    [bk_hash]                             CHAR (32)    NOT NULL,
    [row_id]                              INT          NULL,
    [attribute_set_id]                    INT          NULL,
    [created_at]                          DATETIME     NULL,
    [created_dim_date_key]                CHAR (8)     NULL,
    [created_dim_time_key]                CHAR (8)     NULL,
    [entity_id]                           INT          NULL,
    [has_options_flag]                    CHAR (1)     NULL,
    [required_options_flag]               CHAR (1)     NULL,
    [sku]                                 VARCHAR (64) NULL,
    [type_id]                             VARCHAR (32) NULL,
    [updated_at]                          DATETIME     NULL,
    [updated_dim_date_key]                CHAR (8)     NULL,
    [updated_dim_time_key]                CHAR (8)     NULL,
    [p_magento_catalog_product_entity_id] BIGINT       NOT NULL,
    [deleted_flag]                        INT          NULL,
    [dv_load_date_time]                   DATETIME     NULL,
    [dv_load_end_date_time]               DATETIME     NULL,
    [dv_batch_id]                         BIGINT       NOT NULL,
    [dv_inserted_date_time]               DATETIME     NOT NULL,
    [dv_insert_user]                      VARCHAR (50) NOT NULL,
    [dv_updated_date_time]                DATETIME     NULL,
    [dv_update_user]                      VARCHAR (50) NULL,
    [default_dim_reporting_hierarchy_key] VARCHAR (32) NULL
)
WITH (CLUSTERED COLUMNSTORE INDEX, DISTRIBUTION = HASH([bk_hash]));

