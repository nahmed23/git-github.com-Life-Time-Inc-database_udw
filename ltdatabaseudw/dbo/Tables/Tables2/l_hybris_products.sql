CREATE TABLE [dbo].[l_hybris_products] (
    [l_hybris_products_id]         BIGINT       IDENTITY (1, 1) NOT NULL,
    [bk_hash]                      CHAR (32)    NOT NULL,
    [type_pk_string]               BIGINT       NULL,
    [products_pk]                  BIGINT       NULL,
    [owner_pk_string]              BIGINT       NULL,
    [p_variant_type]               BIGINT       NULL,
    [p_state]                      BIGINT       NULL,
    [p_catalog_version]            BIGINT       NULL,
    [p_media_container]            BIGINT       NULL,
    [p_simple_asset]               BIGINT       NULL,
    [p_europe_1_price_factory_pdg] BIGINT       NULL,
    [p_fulfillment_partner]        BIGINT       NULL,
    [p_base_product]               BIGINT       NULL,
    [unit_pk]                      BIGINT       NULL,
    [p_previous_base_product]      BIGINT       NULL,
    [p_thumb_nail]                 BIGINT       NULL,
    [p_club]                       BIGINT       NULL,
    [p_media]                      BIGINT       NULL,
    [p_content_unit]               BIGINT       NULL,
    [p_raw_asset]                  BIGINT       NULL,
    [p_europe_1_price_factory_ptg] BIGINT       NULL,
    [p_picture]                    BIGINT       NULL,
    [p_approval_status]            BIGINT       NULL,
    [p_europe_1_price_factory_ppg] BIGINT       NULL,
    [p_catalog]                    BIGINT       NULL,
    [p_product_order_limit]        BIGINT       NULL,
    [p_sequence_id]                BIGINT       NULL,
    [p_card_type]                  BIGINT       NULL,
    [p_personal_details]           BIGINT       NULL,
    [p_training_session]           BIGINT       NULL,
    [p_article]                    BIGINT       NULL,
    [p_agreement]                  BIGINT       NULL,
    [p_gender]                     BIGINT       NULL,
    [p_product_tag]                BIGINT       NULL,
    [dv_load_date_time]            DATETIME     NOT NULL,
    [dv_r_load_source_id]          BIGINT       NOT NULL,
    [dv_inserted_date_time]        DATETIME     NOT NULL,
    [dv_insert_user]               VARCHAR (50) NOT NULL,
    [dv_updated_date_time]         DATETIME     NULL,
    [dv_update_user]               VARCHAR (50) NULL,
    [dv_hash]                      CHAR (32)    NOT NULL,
    [dv_batch_id]                  BIGINT       NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));


GO
CREATE CLUSTERED INDEX [ci_l_hybris_products]
    ON [dbo].[l_hybris_products]([bk_hash] ASC, [l_hybris_products_id] ASC);

