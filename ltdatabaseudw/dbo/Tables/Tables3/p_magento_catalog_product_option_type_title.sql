CREATE TABLE [dbo].[p_magento_catalog_product_option_type_title] (
    [p_magento_catalog_product_option_type_title_id] BIGINT       IDENTITY (1, 1) NOT NULL,
    [bk_hash]                                        CHAR (32)    NOT NULL,
    [option_type_title_id]                           INT          NULL,
    [l_magento_catalog_product_option_type_title_id] INT          NULL,
    [s_magento_catalog_product_option_type_title_id] INT          NULL,
    [dv_load_date_time]                              DATETIME     NOT NULL,
    [dv_load_end_date_time]                          DATETIME     NOT NULL,
    [dv_greatest_satellite_date_time]                DATETIME     NULL,
    [dv_next_greatest_satellite_date_time]           DATETIME     NULL,
    [dv_first_in_key_series]                         INT          NULL,
    [dv_inserted_date_time]                          DATETIME     NOT NULL,
    [dv_insert_user]                                 VARCHAR (50) NOT NULL,
    [dv_updated_date_time]                           DATETIME     NULL,
    [dv_update_user]                                 VARCHAR (50) NULL,
    [dv_batch_id]                                    BIGINT       NOT NULL
)
WITH (CLUSTERED INDEX([bk_hash]), DISTRIBUTION = HASH([bk_hash]));

