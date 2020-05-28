CREATE TABLE [dbo].[stage_magento_catalog_product_option_title] (
    [stage_magento_catalog_product_option_title_id] BIGINT        NOT NULL,
    [option_title_id]                               INT           NULL,
    [option_id]                                     INT           NULL,
    [store_id]                                      INT           NULL,
    [title]                                         VARCHAR (255) NULL,
    [dummy_modified_date_time]                      DATETIME      NULL,
    [dv_batch_id]                                   BIGINT        NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

