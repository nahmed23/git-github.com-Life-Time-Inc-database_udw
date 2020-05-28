CREATE TABLE [dbo].[stage_magento_eav_attribute_label] (
    [stage_magento_eav_attribute_label_id] BIGINT        NOT NULL,
    [attribute_label_id]                   INT           NULL,
    [attribute_id]                         INT           NULL,
    [store_id]                             INT           NULL,
    [value]                                VARCHAR (255) NULL,
    [dummy_modified_date_time]             DATETIME      NULL,
    [dv_batch_id]                          BIGINT        NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

