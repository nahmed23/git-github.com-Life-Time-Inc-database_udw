CREATE TABLE [dbo].[stage_hash_magento_eav_attribute_label] (
    [stage_hash_magento_eav_attribute_label_id] BIGINT        IDENTITY (1, 1) NOT NULL,
    [bk_hash]                                   CHAR (32)     NOT NULL,
    [attribute_label_id]                        INT           NULL,
    [attribute_id]                              INT           NULL,
    [store_id]                                  INT           NULL,
    [value]                                     VARCHAR (255) NULL,
    [dummy_modified_date_time]                  DATETIME      NULL,
    [dv_load_date_time]                         DATETIME      NOT NULL,
    [dv_batch_id]                               BIGINT        NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

