CREATE TABLE [dbo].[l_hybris_base_store] (
    [l_hybris_base_store_id]            BIGINT       IDENTITY (1, 1) NOT NULL,
    [bk_hash]                           CHAR (32)    NOT NULL,
    [type_pk_string]                    BIGINT       NULL,
    [owner_pk_string]                   BIGINT       NULL,
    [base_store_pk]                     BIGINT       NULL,
    [p_store_locator_distance_unit]     BIGINT       NULL,
    [p_tax_group]                       BIGINT       NULL,
    [p_default_language]                BIGINT       NULL,
    [p_default_currency]                BIGINT       NULL,
    [p_default_delivery_origin]         BIGINT       NULL,
    [p_solr_facet_search_configuration] BIGINT       NULL,
    [p_pickup_in_store_mode]            BIGINT       NULL,
    [p_default_atp_formula]             BIGINT       NULL,
    [p_sourcing_config]                 BIGINT       NULL,
    [dv_load_date_time]                 DATETIME     NOT NULL,
    [dv_r_load_source_id]               BIGINT       NOT NULL,
    [dv_inserted_date_time]             DATETIME     NOT NULL,
    [dv_insert_user]                    VARCHAR (50) NOT NULL,
    [dv_updated_date_time]              DATETIME     NULL,
    [dv_update_user]                    VARCHAR (50) NULL,
    [dv_hash]                           CHAR (32)    NOT NULL,
    [dv_batch_id]                       BIGINT       NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));


GO
CREATE CLUSTERED INDEX [ci_l_hybris_base_store]
    ON [dbo].[l_hybris_base_store]([bk_hash] ASC, [l_hybris_base_store_id] ASC);

