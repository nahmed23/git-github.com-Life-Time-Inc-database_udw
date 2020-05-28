CREATE TABLE [dbo].[d_magento_customer_entity_text] (
    [d_magento_customer_entity_text_id] BIGINT        IDENTITY (1, 1) NOT NULL,
    [bk_hash]                           CHAR (32)     NOT NULL,
    [value_id]                          INT           NULL,
    [d_magento_customer_entity_bk_hash] CHAR (32)     NULL,
    [d_magento_eav_attribute_bk_hash]   VARCHAR (32)  NULL,
    [value]                             VARCHAR (255) NULL,
    [p_magento_customer_entity_text_id] BIGINT        NOT NULL,
    [deleted_flag]                      INT           NULL,
    [dv_load_date_time]                 DATETIME      NULL,
    [dv_load_end_date_time]             DATETIME      NULL,
    [dv_batch_id]                       BIGINT        NOT NULL,
    [dv_inserted_date_time]             DATETIME      NOT NULL,
    [dv_insert_user]                    VARCHAR (50)  NOT NULL,
    [dv_updated_date_time]              DATETIME      NULL,
    [dv_update_user]                    VARCHAR (50)  NULL
)
WITH (CLUSTERED COLUMNSTORE INDEX, DISTRIBUTION = HASH([bk_hash]));

