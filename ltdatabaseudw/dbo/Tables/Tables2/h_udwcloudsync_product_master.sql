CREATE TABLE [dbo].[h_udwcloudsync_product_master] (
    [h_udwcloudsync_product_master_id] BIGINT          IDENTITY (1, 1) NOT NULL,
    [bk_hash]                          CHAR (32)       NOT NULL,
    [product_id]                       NVARCHAR (4000) NULL,
    [product_sku]                      NVARCHAR (4000) NULL,
    [source_system_link_title]         NVARCHAR (4000) NULL,
    [dv_load_date_time]                DATETIME        NOT NULL,
    [dv_r_load_source_id]              BIGINT          NOT NULL,
    [dv_inserted_date_time]            DATETIME        NOT NULL,
    [dv_insert_user]                   VARCHAR (50)    NOT NULL,
    [dv_updated_date_time]             DATETIME        NULL,
    [dv_update_user]                   VARCHAR (50)    NULL,
    [dv_deleted]                       BIT             DEFAULT ((0)) NOT NULL,
    [dv_batch_id]                      BIGINT          NOT NULL
)
WITH (CLUSTERED INDEX([bk_hash]), DISTRIBUTION = HASH([bk_hash]));

