CREATE TABLE [dbo].[stage_magento_catalog_product_bundle_option] (
    [stage_magento_catalog_product_bundle_option_id] BIGINT         NOT NULL,
    [option_id]                                      INT            NULL,
    [parent_id]                                      INT            NULL,
    [required]                                       INT            NULL,
    [position]                                       INT            NULL,
    [type]                                           NVARCHAR (255) NULL,
    [dummy_modified_date_time]                       DATETIME       NULL,
    [dv_batch_id]                                    BIGINT         NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

