CREATE TABLE [dbo].[stage_hash_magento_catalog_product_bundle_option] (
    [stage_hash_magento_catalog_product_bundle_option_id] BIGINT         IDENTITY (1, 1) NOT NULL,
    [bk_hash]                                             CHAR (32)      NOT NULL,
    [option_id]                                           INT            NULL,
    [parent_id]                                           INT            NULL,
    [required]                                            INT            NULL,
    [position]                                            INT            NULL,
    [type]                                                NVARCHAR (255) NULL,
    [dummy_modified_date_time]                            DATETIME       NULL,
    [dv_load_date_time]                                   DATETIME       NOT NULL,
    [dv_batch_id]                                         BIGINT         NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

