CREATE TABLE [dbo].[d_ig_ig_dimension_menu_item_dimension] (
    [d_ig_ig_dimension_menu_item_dimension_id] BIGINT        IDENTITY (1, 1) NOT NULL,
    [bk_hash]                                  CHAR (32)     NOT NULL,
    [menu_item_dim_id]                         BIGINT        NULL,
    [dim_cafe_product_key]                     CHAR (32)     NULL,
    [effective_dim_date_key]                   CHAR (8)      NULL,
    [expiration_dim_date_key]                  CHAR (8)      NULL,
    [menu_item_id]                             INT           NULL,
    [menu_item_name]                           NVARCHAR (50) NULL,
    [product_class_id]                         INT           NULL,
    [product_class_name]                       NVARCHAR (50) NULL,
    [sku_number]                               NVARCHAR (30) NULL,
    [p_ig_ig_dimension_menu_item_dimension_id] BIGINT        NOT NULL,
    [deleted_flag]                             INT           NULL,
    [dv_load_date_time]                        DATETIME      NULL,
    [dv_load_end_date_time]                    DATETIME      NULL,
    [dv_batch_id]                              BIGINT        NOT NULL,
    [dv_inserted_date_time]                    DATETIME      NOT NULL,
    [dv_insert_user]                           VARCHAR (50)  NOT NULL,
    [dv_updated_date_time]                     DATETIME      NULL,
    [dv_update_user]                           VARCHAR (50)  NULL,
    [default_dim_reporting_hierarchy_key]      VARCHAR (32)  NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));


GO
CREATE NONCLUSTERED INDEX [ix_dv_batch_id]
    ON [dbo].[d_ig_ig_dimension_menu_item_dimension]([dv_batch_id] ASC);

