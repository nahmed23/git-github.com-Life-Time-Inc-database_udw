CREATE TABLE [dbo].[stage_magento_eav_attribute_set] (
    [stage_magento_eav_attribute_set_id] BIGINT        NOT NULL,
    [attribute_set_id]                   INT           NULL,
    [entity_type_id]                     INT           NULL,
    [attribute_set_name]                 VARCHAR (255) NULL,
    [sort_order]                         INT           NULL,
    [dummy_modified_date_time]           DATETIME      NULL,
    [dv_batch_id]                        BIGINT        NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

