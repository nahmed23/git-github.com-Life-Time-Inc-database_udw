CREATE TABLE [dbo].[l_hybris_order_entries] (
    [l_hybris_order_entries_id]    BIGINT       IDENTITY (1, 1) NOT NULL,
    [bk_hash]                      CHAR (32)    NOT NULL,
    [type_pk_string]               BIGINT       NULL,
    [owner_pk_string]              BIGINT       NULL,
    [order_entries_pk]             BIGINT       NULL,
    [p_product]                    BIGINT       NULL,
    [p_unit]                       BIGINT       NULL,
    [p_order]                      BIGINT       NULL,
    [p_europe_1_price_factory_ppg] BIGINT       NULL,
    [p_europe_1_price_factory_ptg] BIGINT       NULL,
    [p_europe_1_price_factory_pdg] BIGINT       NULL,
    [p_chosen_vendor]              BIGINT       NULL,
    [p_delivery_address]           BIGINT       NULL,
    [p_delivery_mode]              BIGINT       NULL,
    [p_quantity_status]            BIGINT       NULL,
    [p_delivery_point_of_service]  BIGINT       NULL,
    [p_original_order_entry]       BIGINT       NULL,
    [p_master_entry]               BIGINT       NULL,
    [p_bundle_template]            BIGINT       NULL,
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
CREATE CLUSTERED INDEX [ci_l_hybris_order_entries]
    ON [dbo].[l_hybris_order_entries]([bk_hash] ASC, [l_hybris_order_entries_id] ASC);

