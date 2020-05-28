CREATE TABLE [dbo].[d_magento_catalog_product_bundle_selection] (
    [d_magento_catalog_product_bundle_selection_id] BIGINT          IDENTITY (1, 1) NOT NULL,
    [bk_hash]                                       CHAR (32)       NOT NULL,
    [selection_id]                                  INT             NULL,
    [d_magento_parent_product_bk_hash]              VARCHAR (32)    NULL,
    [default_flag]                                  CHAR (1)        NULL,
    [option_id]                                     INT             NULL,
    [position]                                      INT             NULL,
    [product_id]                                    INT             NULL,
    [selection_can_change_qty]                      INT             NULL,
    [selection_price_type]                          INT             NULL,
    [selection_price_value]                         DECIMAL (12, 4) NULL,
    [selection_qty]                                 DECIMAL (12, 4) NULL,
    [p_magento_catalog_product_bundle_selection_id] BIGINT          NOT NULL,
    [deleted_flag]                                  INT             NULL,
    [dv_load_date_time]                             DATETIME        NULL,
    [dv_load_end_date_time]                         DATETIME        NULL,
    [dv_batch_id]                                   BIGINT          NOT NULL,
    [dv_inserted_date_time]                         DATETIME        NOT NULL,
    [dv_insert_user]                                VARCHAR (50)    NOT NULL,
    [dv_updated_date_time]                          DATETIME        NULL,
    [dv_update_user]                                VARCHAR (50)    NULL
)
WITH (CLUSTERED COLUMNSTORE INDEX, DISTRIBUTION = HASH([bk_hash]));

