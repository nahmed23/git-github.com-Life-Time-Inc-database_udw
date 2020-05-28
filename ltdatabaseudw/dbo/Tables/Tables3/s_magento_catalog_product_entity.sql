CREATE TABLE [dbo].[s_magento_catalog_product_entity] (
    [s_magento_catalog_product_entity_id] BIGINT       IDENTITY (1, 1) NOT NULL,
    [bk_hash]                             CHAR (32)    NOT NULL,
    [row_id]                              INT          NULL,
    [created_in]                          BIGINT       NULL,
    [updated_in]                          BIGINT       NULL,
    [type_id]                             VARCHAR (32) NULL,
    [has_options]                         INT          NULL,
    [required_options]                    INT          NULL,
    [created_at]                          DATETIME     NULL,
    [updated_at]                          DATETIME     NULL,
    [dv_load_date_time]                   DATETIME     NOT NULL,
    [dv_r_load_source_id]                 BIGINT       NOT NULL,
    [dv_inserted_date_time]               DATETIME     NOT NULL,
    [dv_insert_user]                      VARCHAR (50) NOT NULL,
    [dv_updated_date_time]                DATETIME     NULL,
    [dv_update_user]                      VARCHAR (50) NULL,
    [dv_hash]                             CHAR (32)    NOT NULL,
    [dv_deleted]                          BIT          DEFAULT ((0)) NOT NULL,
    [dv_batch_id]                         BIGINT       NOT NULL
)
WITH (CLUSTERED INDEX([bk_hash]), DISTRIBUTION = HASH([bk_hash]));

