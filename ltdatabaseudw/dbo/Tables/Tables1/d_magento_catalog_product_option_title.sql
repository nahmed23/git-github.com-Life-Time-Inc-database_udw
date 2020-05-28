CREATE TABLE [dbo].[d_magento_catalog_product_option_title] (
    [d_magento_catalog_product_option_title_id] BIGINT        IDENTITY (1, 1) NOT NULL,
    [bk_hash]                                   CHAR (32)     NOT NULL,
    [option_title_id]                           INT           NULL,
    [d_magento_option_bk_hash]                  CHAR (32)     NULL,
    [store_id]                                  INT           NULL,
    [title]                                     VARCHAR (255) NULL,
    [p_magento_catalog_product_option_title_id] BIGINT        NOT NULL,
    [deleted_flag]                              INT           NULL,
    [dv_load_date_time]                         DATETIME      NULL,
    [dv_load_end_date_time]                     DATETIME      NULL,
    [dv_batch_id]                               BIGINT        NOT NULL,
    [dv_inserted_date_time]                     DATETIME      NOT NULL,
    [dv_insert_user]                            VARCHAR (50)  NOT NULL,
    [dv_updated_date_time]                      DATETIME      NULL,
    [dv_update_user]                            VARCHAR (50)  NULL
)
WITH (CLUSTERED COLUMNSTORE INDEX, DISTRIBUTION = HASH([bk_hash]));

