CREATE TABLE [dbo].[d_magento_lifetime_order_item_change_log] (
    [d_magento_lifetime_order_item_change_log_id] BIGINT        IDENTITY (1, 1) NOT NULL,
    [bk_hash]                                     CHAR (32)     NOT NULL,
    [entity_id]                                   INT           NULL,
    [dim_mms_product_key]                         VARCHAR (32)  NULL,
    [fact_magento_order_item_key]                 VARCHAR (32)  NULL,
    [fact_mms_transaction_key]                    VARCHAR (32)  NULL,
    [mms_tran_id]                                 VARCHAR (255) NULL,
    [status]                                      INT           NULL,
    [transaction_type]                            VARCHAR (255) NULL,
    [p_magento_lifetime_order_item_change_log_id] BIGINT        NOT NULL,
    [deleted_flag]                                INT           NULL,
    [dv_load_date_time]                           DATETIME      NULL,
    [dv_load_end_date_time]                       DATETIME      NULL,
    [dv_batch_id]                                 BIGINT        NOT NULL,
    [dv_inserted_date_time]                       DATETIME      NOT NULL,
    [dv_insert_user]                              VARCHAR (50)  NOT NULL,
    [dv_updated_date_time]                        DATETIME      NULL,
    [dv_update_user]                              VARCHAR (50)  NULL
)
WITH (CLUSTERED COLUMNSTORE INDEX, DISTRIBUTION = HASH([bk_hash]));

