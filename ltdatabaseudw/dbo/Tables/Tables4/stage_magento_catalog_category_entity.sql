CREATE TABLE [dbo].[stage_magento_catalog_category_entity] (
    [stage_magento_catalog_category_entity_id] BIGINT        NOT NULL,
    [row_id]                                   INT           NULL,
    [entity_id]                                INT           NULL,
    [created_in]                               BIGINT        NULL,
    [updated_in]                               BIGINT        NULL,
    [attribute_set_id]                         INT           NULL,
    [parent_id]                                INT           NULL,
    [created_at]                               DATETIME      NULL,
    [updated_at]                               DATETIME      NULL,
    [path]                                     VARCHAR (255) NULL,
    [position]                                 INT           NULL,
    [level]                                    INT           NULL,
    [children_count]                           INT           NULL,
    [dv_batch_id]                              BIGINT        NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

