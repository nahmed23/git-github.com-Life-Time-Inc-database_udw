CREATE TABLE [dbo].[dim_cafe_product] (
    [dim_cafe_product_id]                 BIGINT        IDENTITY (1, 1) NOT NULL,
    [dim_cafe_product_key]                CHAR (32)     NULL,
    [kronos_labor_category]               NVARCHAR (10) NULL,
    [menu_item_active_flag]               CHAR (1)      NULL,
    [menu_item_id]                        INT           NULL,
    [menu_item_name]                      NVARCHAR (50) NULL,
    [product_class_id]                    INT           NULL,
    [product_class_name]                  NVARCHAR (50) NULL,
    [sku_number]                          NVARCHAR (30) NULL,
    [dv_load_date_time]                   DATETIME      NULL,
    [dv_load_end_date_time]               DATETIME      NULL,
    [dv_batch_id]                         BIGINT        NOT NULL,
    [dv_inserted_date_time]               DATETIME      NOT NULL,
    [dv_insert_user]                      VARCHAR (50)  NOT NULL,
    [dv_updated_date_time]                DATETIME      NULL,
    [dv_update_user]                      VARCHAR (50)  NULL,
    [default_dim_reporting_hierarchy_key] VARCHAR (32)  NULL
)
WITH (HEAP, DISTRIBUTION = HASH([dim_cafe_product_key]));

