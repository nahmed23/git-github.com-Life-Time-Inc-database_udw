CREATE TABLE [dbo].[stage_hash_magento_catalog_product_entity] (
    [stage_hash_magento_catalog_product_entity_id] BIGINT       IDENTITY (1, 1) NOT NULL,
    [bk_hash]                                      CHAR (32)    NOT NULL,
    [row_id]                                       INT          NULL,
    [entity_id]                                    INT          NULL,
    [created_in]                                   BIGINT       NULL,
    [updated_in]                                   BIGINT       NULL,
    [attribute_set_id]                             INT          NULL,
    [type_id]                                      VARCHAR (32) NULL,
    [sku]                                          VARCHAR (64) NULL,
    [has_options]                                  INT          NULL,
    [required_options]                             INT          NULL,
    [created_at]                                   DATETIME     NULL,
    [updated_at]                                   DATETIME     NULL,
    [dv_load_date_time]                            DATETIME     NOT NULL,
    [dv_batch_id]                                  BIGINT       NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

