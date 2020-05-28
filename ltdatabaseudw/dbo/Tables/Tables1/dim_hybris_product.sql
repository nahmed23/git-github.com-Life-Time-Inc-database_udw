CREATE TABLE [dbo].[dim_hybris_product] (
    [dim_hybris_product_id]           BIGINT         IDENTITY (1, 1) NOT NULL,
    [dim_hybris_product_key]          CHAR (32)      NULL,
    [products_pk]                     BIGINT         NULL,
    [catalog_name]                    NVARCHAR (255) NULL,
    [catalog_version]                 NVARCHAR (255) NULL,
    [created_date_time]               DATETIME       NULL,
    [dim_mms_product_key]             CHAR (32)      NULL,
    [fulfillment_dim_mms_product_key] CHAR (32)      NULL,
    [modified_date_time]              DATETIME       NULL,
    [offline_date]                    DATETIME       NULL,
    [online_date]                     DATETIME       NULL,
    [product_display_name]            NVARCHAR (255) NULL,
    [product_name]                    NVARCHAR (255) NULL,
    [unit_of_measure]                 NVARCHAR (255) NULL,
    [dv_load_date_time]               DATETIME       NULL,
    [dv_load_end_date_time]           DATETIME       NULL,
    [dv_batch_id]                     BIGINT         NULL,
    [dv_inserted_date_time]           DATETIME       NOT NULL,
    [dv_insert_user]                  VARCHAR (50)   NOT NULL,
    [dv_updated_date_time]            DATETIME       NULL,
    [dv_update_user]                  VARCHAR (50)   NULL
)
WITH (HEAP, DISTRIBUTION = HASH([dim_hybris_product_key]));

