CREATE TABLE [dbo].[l_hybris_users] (
    [l_hybris_users_id]            BIGINT       IDENTITY (1, 1) NOT NULL,
    [bk_hash]                      CHAR (32)    NOT NULL,
    [type_pk_string]               BIGINT       NULL,
    [owner_pk_string]              BIGINT       NULL,
    [users_pk]                     BIGINT       NULL,
    [p_profile_picture]            BIGINT       NULL,
    [p_default_payment_address]    BIGINT       NULL,
    [p_default_shipment_address]   BIGINT       NULL,
    [p_session_language]           BIGINT       NULL,
    [p_session_currency]           BIGINT       NULL,
    [p_user_profile]               BIGINT       NULL,
    [p_europe_1_price_factory_udg] BIGINT       NULL,
    [p_europe_1_price_factory_upg] BIGINT       NULL,
    [p_europe_1_price_factory_utg] BIGINT       NULL,
    [p_title]                      BIGINT       NULL,
    [p_default_payment_info]       BIGINT       NULL,
    [p_type]                       BIGINT       NULL,
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
CREATE CLUSTERED INDEX [ci_l_hybris_users]
    ON [dbo].[l_hybris_users]([bk_hash] ASC, [l_hybris_users_id] ASC);

