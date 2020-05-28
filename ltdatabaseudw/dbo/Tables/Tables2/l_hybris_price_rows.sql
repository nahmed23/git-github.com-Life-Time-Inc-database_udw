CREATE TABLE [dbo].[l_hybris_price_rows] (
    [l_hybris_price_rows_id]    BIGINT       IDENTITY (1, 1) NOT NULL,
    [bk_hash]                   CHAR (32)    NOT NULL,
    [type_pk_string]            BIGINT       NULL,
    [owner_pk_string]           BIGINT       NULL,
    [price_rows_pk]             BIGINT       NULL,
    [p_product]                 BIGINT       NULL,
    [p_pg]                      BIGINT       NULL,
    [p_product_match_qualifier] BIGINT       NULL,
    [p_user]                    BIGINT       NULL,
    [p_ug]                      BIGINT       NULL,
    [p_catalog_version]         BIGINT       NULL,
    [p_currency]                BIGINT       NULL,
    [p_unit]                    BIGINT       NULL,
    [p_channel]                 BIGINT       NULL,
    [p_sequence_id]             BIGINT       NULL,
    [dv_load_date_time]         DATETIME     NOT NULL,
    [dv_r_load_source_id]       BIGINT       NOT NULL,
    [dv_inserted_date_time]     DATETIME     NOT NULL,
    [dv_insert_user]            VARCHAR (50) NOT NULL,
    [dv_updated_date_time]      DATETIME     NULL,
    [dv_update_user]            VARCHAR (50) NULL,
    [dv_hash]                   CHAR (32)    NOT NULL,
    [dv_batch_id]               BIGINT       NOT NULL
)
WITH (HEAP, DISTRIBUTION = HASH([bk_hash]));


GO
CREATE CLUSTERED INDEX [ci_l_hybris_price_rows]
    ON [dbo].[l_hybris_price_rows]([bk_hash] ASC, [l_hybris_price_rows_id] ASC);

