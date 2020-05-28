CREATE TABLE [dbo].[d_lt_bucks_products] (
    [d_lt_bucks_products_id]       BIGINT          IDENTITY (1, 1) NOT NULL,
    [bk_hash]                      CHAR (32)       NOT NULL,
    [dim_products_key]             CHAR (32)       NULL,
    [product_id]                   INT             NULL,
    [created_date_time]            DATETIME        NULL,
    [date_updated]                 DATETIME        NULL,
    [last_modified_timestamp]      DATETIME        NULL,
    [price]                        DECIMAL (26, 6) NULL,
    [product_active_flag]          CHAR (1)        NULL,
    [product_description]          NVARCHAR (4000) NULL,
    [product_is_soft_deleted_flag] CHAR (1)        NULL,
    [product_name]                 NVARCHAR (100)  NULL,
    [product_per]                  VARCHAR (20)    NULL,
    [sku]                          NVARCHAR (20)   NULL,
    [p_lt_bucks_products_id]       BIGINT          NOT NULL,
    [dv_load_date_time]            DATETIME        NULL,
    [dv_load_end_date_time]        DATETIME        NULL,
    [dv_batch_id]                  BIGINT          NOT NULL,
    [dv_inserted_date_time]        DATETIME        NOT NULL,
    [dv_insert_user]               VARCHAR (50)    NOT NULL,
    [dv_updated_date_time]         DATETIME        NULL,
    [dv_update_user]               VARCHAR (50)    NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));


GO
CREATE NONCLUSTERED INDEX [ix_dv_batch_id]
    ON [dbo].[d_lt_bucks_products]([dv_batch_id] ASC);

