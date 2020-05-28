CREATE TABLE [dbo].[stage_hash_magento_customer_address_entity_varchar] (
    [stage_hash_magento_customer_address_entity_varchar_id] BIGINT        IDENTITY (1, 1) NOT NULL,
    [bk_hash]                                               CHAR (32)     NOT NULL,
    [value_id]                                              INT           NULL,
    [attribute_id]                                          INT           NULL,
    [entity_id]                                             INT           NULL,
    [value]                                                 VARCHAR (255) NULL,
    [dummy_modified_date_time]                              DATETIME      NULL,
    [dv_load_date_time]                                     DATETIME      NOT NULL,
    [dv_batch_id]                                           BIGINT        NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);

