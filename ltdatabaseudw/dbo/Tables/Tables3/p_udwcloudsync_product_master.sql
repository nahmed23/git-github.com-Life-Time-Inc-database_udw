CREATE TABLE [dbo].[p_udwcloudsync_product_master] (
    [p_udwcloudsync_product_master_id]     BIGINT          IDENTITY (1, 1) NOT NULL,
    [bk_hash]                              CHAR (32)       NOT NULL,
    [product_id]                           NVARCHAR (4000) NULL,
    [product_sku]                          NVARCHAR (4000) NULL,
    [source_system_link_title]             NVARCHAR (4000) NULL,
    [l_udwcloudsync_product_master_id]     BIGINT          NULL,
    [s_udwcloudsync_product_master_id]     BIGINT          NULL,
    [dv_load_date_time]                    DATETIME        NOT NULL,
    [dv_load_end_date_time]                DATETIME        NOT NULL,
    [dv_greatest_satellite_date_time]      DATETIME        NULL,
    [dv_next_greatest_satellite_date_time] DATETIME        NULL,
    [dv_first_in_key_series]               INT             NULL,
    [dv_inserted_date_time]                DATETIME        NOT NULL,
    [dv_insert_user]                       VARCHAR (50)    NOT NULL,
    [dv_updated_date_time]                 DATETIME        NULL,
    [dv_update_user]                       VARCHAR (50)    NULL,
    [dv_batch_id]                          BIGINT          NOT NULL
)
WITH (CLUSTERED INDEX([bk_hash]), DISTRIBUTION = HASH([bk_hash]));


GO
CREATE NONCLUSTERED INDEX [ix_dv_batch_id]
    ON [dbo].[p_udwcloudsync_product_master]([dv_batch_id] ASC);

