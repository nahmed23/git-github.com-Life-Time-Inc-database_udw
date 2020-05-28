CREATE TABLE [dbo].[d_magento_customer_eav_attribute_website] (
    [d_magento_customer_eav_attribute_website_id] BIGINT       IDENTITY (1, 1) NOT NULL,
    [bk_hash]                                     CHAR (32)    NOT NULL,
    [attribute_id]                                INT          NULL,
    [website_id]                                  INT          NULL,
    [d_magento_customer_eav_attribute_bk_hash]    CHAR (32)    NULL,
    [is_required]                                 INT          NULL,
    [is_visible]                                  INT          NULL,
    [multiline_count]                             INT          NULL,
    [p_magento_customer_eav_attribute_website_id] BIGINT       NOT NULL,
    [deleted_flag]                                INT          NULL,
    [dv_load_date_time]                           DATETIME     NULL,
    [dv_load_end_date_time]                       DATETIME     NULL,
    [dv_batch_id]                                 BIGINT       NOT NULL,
    [dv_inserted_date_time]                       DATETIME     NOT NULL,
    [dv_insert_user]                              VARCHAR (50) NOT NULL,
    [dv_updated_date_time]                        DATETIME     NULL,
    [dv_update_user]                              VARCHAR (50) NULL
)
WITH (CLUSTERED COLUMNSTORE INDEX, DISTRIBUTION = HASH([bk_hash]));

