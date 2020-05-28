CREATE TABLE [dbo].[s_hybris_products_lp] (
    [s_hybris_products_lp_id]         BIGINT          IDENTITY (1, 1) NOT NULL,
    [bk_hash]                         CHAR (32)       NOT NULL,
    [item_pk]                         BIGINT          NULL,
    [item_type_pk]                    BIGINT          NULL,
    [lang_pk]                         BIGINT          NULL,
    [p_specifications]                BIGINT          NULL,
    [p_size]                          NVARCHAR (255)  NULL,
    [p_description]                   VARCHAR (8000)  NULL,
    [p_offer_media]                   BIGINT          NULL,
    [p_localized_asset]               BIGINT          NULL,
    [p_summary]                       VARCHAR (8000)  NULL,
    [p_caption]                       NVARCHAR (255)  NULL,
    [p_localized_pdf_asset]           BIGINT          NULL,
    [p_nutrition_weight]              NVARCHAR (255)  NULL,
    [p_flavor]                        NVARCHAR (255)  NULL,
    [p_localized_asset_picture]       BIGINT          NULL,
    [p_manufacturer_type_description] NVARCHAR (255)  NULL,
    [p_offer_link]                    NVARCHAR (1000) NULL,
    [p_style]                         NVARCHAR (255)  NULL,
    [p_segment]                       NVARCHAR (255)  NULL,
    [p_name]                          NVARCHAR (255)  NULL,
    [p_restriction]                   VARCHAR (8000)  NULL,
    [p_instruction]                   VARCHAR (8000)  NULL,
    [p_other]                         BIGINT          NULL,
    [p_option]                        NVARCHAR (255)  NULL,
    [p_instructions]                  VARCHAR (8000)  NULL,
    [p_package]                       NVARCHAR (255)  NULL,
    [p_type]                          NVARCHAR (255)  NULL,
    [p_version]                       NVARCHAR (255)  NULL,
    [p_resistance]                    NVARCHAR (255)  NULL,
    [p_model]                         NVARCHAR (255)  NULL,
    [p_weight_variant]                NVARCHAR (255)  NULL,
    [p_scent]                         NVARCHAR (255)  NULL,
    [p_color]                         NVARCHAR (255)  NULL,
    [created_ts]                      DATETIME        NULL,
    [modified_ts]                     DATETIME        NULL,
    [dv_load_date_time]               DATETIME        NOT NULL,
    [dv_r_load_source_id]             BIGINT          NOT NULL,
    [dv_inserted_date_time]           DATETIME        NOT NULL,
    [dv_insert_user]                  VARCHAR (50)    NOT NULL,
    [dv_updated_date_time]            DATETIME        NULL,
    [dv_update_user]                  VARCHAR (50)    NULL,
    [dv_hash]                         CHAR (32)       NOT NULL,
    [dv_batch_id]                     BIGINT          NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));


GO
CREATE CLUSTERED INDEX [ci_s_hybris_products_lp]
    ON [dbo].[s_hybris_products_lp]([bk_hash] ASC, [s_hybris_products_lp_id] ASC);

