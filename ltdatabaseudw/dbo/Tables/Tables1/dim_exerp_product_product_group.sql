CREATE TABLE [dbo].[dim_exerp_product_product_group] (
    [dim_exerp_product_product_group_id]  BIGINT         IDENTITY (1, 1) NOT NULL,
    [dim_exerp_product_key]               VARCHAR (32)   NULL,
    [dim_exerp_product_product_group_key] VARCHAR (32)   NULL,
    [dimension_product_group_id]          INT            NULL,
    [dimension_product_group_name]        VARCHAR (4000) NULL,
    [parent_product_group_id]             INT            NULL,
    [parent_product_group_name]           VARCHAR (4000) NULL,
    [primary_product_group_flag]          CHAR (1)       NULL,
    [product_group_external_id]           VARCHAR (4000) NULL,
    [product_group_id]                    INT            NULL,
    [product_group_name]                  VARCHAR (4000) NULL,
    [product_id]                          VARCHAR (4000) NULL,
    [dv_load_date_time]                   DATETIME       NULL,
    [dv_load_end_date_time]               DATETIME       NULL,
    [dv_batch_id]                         BIGINT         NOT NULL,
    [dv_inserted_date_time]               DATETIME       NOT NULL,
    [dv_insert_user]                      VARCHAR (50)   NOT NULL,
    [dv_updated_date_time]                DATETIME       NULL,
    [dv_update_user]                      VARCHAR (50)   NULL
)
WITH (CLUSTERED COLUMNSTORE INDEX, DISTRIBUTION = HASH([dim_exerp_product_product_group_key]));

